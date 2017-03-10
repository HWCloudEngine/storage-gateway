/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    ceph_s3_meta.cc
* Author: 
* Date:         2016/07/13
* Version:      1.0
* Description:
* 
***********************************************/
#include <algorithm>    // std::for_each
#include <iostream>
#include <memory>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include "ceph_s3_meta.h"
#include "log/log.h"
#include "common/config_parser.h"
#include "sg_util.h"
const uint64_t MAX_JOURNAL_COUNT = 1000LU; // TODO:config
const uint64_t MIN_JOURNAL_COUNTER = 0LU;

using std::unique_ptr;
using huawei::proto::REP_FAILED_OVER;
using huawei::proto::REP_ENABLED;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::NO_SUCH_KEY;
using huawei::proto::OPENED;
using huawei::proto::SEALED;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATOR;
using huawei::proto::REP_PRIMARY;

static string assemble_journal_marker_key(const string& vol_id,
        const CONSUMER_TYPE& type, const bool is_consumer=true) {
    string key = g_marker_prefix + vol_id + "/";
    if(is_consumer)
        key += g_consumer;
    else
        key += g_producer;
    if(REPLAYER == type){
        key += g_replayer;
    }
    else{
        key += g_replicator;
    }
    return key;
}

static RESULT mkdir_if_not_exist(const char *path, mode_t mode)
{
    struct stat st;
    RESULT status = DRS_OK;
    if (stat(path, &st) != 0)
    {
        /* Directory does not exist. EEXIST for race condition */
        if (mkdir(path, mode) != 0 && errno != EEXIST){
            status = INTERNAL_ERROR;
            LOG_ERROR << "create path " << path << " failed:" << strerror(errno);
        }
    }
    else if (!S_ISDIR(st.st_mode))
    {
        errno = ENOTDIR;
        status = INTERNAL_ERROR;
        LOG_ERROR << path << " is not directory.";
    }

    return(status);
}

RESULT delete_journal_file(const string& name) {
    int res = remove(name.c_str());
    if(0 == res || ENOENT == errno) // file deleted or not exist
        return DRS_OK;
    LOG_ERROR << "delete " << name << " failed:" << strerror(res);
    return INTERNAL_ERROR;
}
static string get_journal_filename(const string& vol_id,const uint64_t& counter){
    return g_key_prefix + vol_id + "/" + sg_util::counter_to_string(counter);
}
string construct_sealed_index(const string& m_key){
    string sub = m_key.substr(g_key_prefix.length(),m_key.length()-g_key_prefix.length());
    return g_sealed + sub; 
}
string construct_recyled_index(const string& m_key){
    string sub = m_key;
    sub.erase(0,sub.find_first_of("/")+1);
    sub.erase(0,sub.find_first_of("/")+1);
    return g_recycled + sub;
}
string construct_write_open_index(const string& m_key, 
        const string& vol_id, const string& uuid){
    return g_writer_prefix + vol_id + "/" + uuid + "/"
        + m_key.substr(m_key.find_last_of('/')+1);
}
string construct_volume_meta_key(const string& uuid){
    return g_volume_prefix + uuid;
}

std::shared_ptr<JournalHeadTail> init_head_tail_by_journals(
            std::list<string>& list){

    std::shared_ptr<JournalHeadTail> head_tail = std::make_shared<JournalHeadTail>();
    uint64_t counter1;
    if(true != sg_util::extract_counter_from_object_key(list.front(),counter1)){
        LOG_ERROR << "extract counter from key [" << list.front() << "] failed!";
        return nullptr;
    }
    uint64_t counter2;
    if(true != sg_util::extract_counter_from_object_key(list.back(),counter2)){
        LOG_ERROR << "extract counter from key [" << list.back() << "] failed!";
        return nullptr;
    }
    if((counter2 >> SUB_COUNTER_BITS) + 1 == MAX_JOURNAL_COUNT){ // tail counter at next loop
        uint64_t expected = 0;
        for(auto it=list.begin();it!=list.end();++it){
            // look for the discontinuous position
            uint64_t missed_counter;
            if(true != sg_util::extract_counter_from_object_key(*it,missed_counter)){
                LOG_ERROR << "extract counter from key [" << *it << "] failed!";
                return nullptr;
            }
            LOG_DEBUG << "extract counter:" << missed_counter;

            if((missed_counter >> SUB_COUNTER_BITS) != expected){
                // discontinuous position found
                head_tail->tail = expected;

                uint64_t head_counter;
                if(true != sg_util::extract_counter_from_object_key(*it,head_counter)){
                    LOG_ERROR << "extract counter from key [" << *it << "] failed!";
                    return nullptr;
                }
                head_tail->head = head_counter >> SUB_COUNTER_BITS;

                LOG_INFO << "init volume journal key counter (front):"
                    << head_counter;
                LOG_INFO << "init volume journal key counter (head/tail):"
                    << head_tail->head << "/" << head_tail->tail;
                break;
            }
            expected++;
        }
    }
    else{        
        head_tail->head = (counter1 >> SUB_COUNTER_BITS) % MAX_JOURNAL_COUNT;
        head_tail->tail = ((counter2 >> SUB_COUNTER_BITS) + 1) % MAX_JOURNAL_COUNT;
        LOG_INFO << "init volume journal key counter (front/rear):"
                    << counter1 << "/" << counter2;
        LOG_INFO << "init volume journal key counter (head/tail):"
                    << head_tail->head << "/" << head_tail->tail;
    }
    //fool proof:counter is sequential
    DR_ASSERT((head_tail->tail + MAX_JOURNAL_COUNT - head_tail->head) % MAX_JOURNAL_COUNT== list.size());
    return head_tail;
}

// sort the journal list according JournalHeadTail
void CephS3Meta::sort_journals(const string& vol_id, std::list<string>& list){

    if(list.empty())
        return;
    std::shared_ptr<JournalHeadTail> head_tail = get_journal_key_counter(vol_id);
    DR_ASSERT(head_tail != nullptr);
    std::lock_guard<std::mutex> lck(head_tail->mtx);

    LOG_DEBUG << "head:" << head_tail->head << ",tail:" << head_tail->tail;

    if(head_tail->head <= head_tail->tail){
        return;
    }
    else{
        auto it=list.begin();
        // move to head element,note:tail point to the element next to back(rear)
        std::advance(it,head_tail->tail);
        // confirm the counter:fool proof; TODO:delete later
        uint64_t head_counter;
        if(true != sg_util::extract_counter_from_object_key(*it,head_counter)){
            LOG_ERROR << "extract counter from key [" << *it << "] failed!";
            return;
        }
        DR_ASSERT((head_counter >> SUB_COUNTER_BITS) == head_tail->head);

        // order
        std::list<string> ordered_list;
        ordered_list.splice(ordered_list.begin(),list,it,list.end());
        ordered_list.splice(ordered_list.end(),list);
        list.swap(ordered_list);
    }

}

RESULT CephS3Meta::create_journal_file(const string& name) {
    string path = name.substr(0,name.find_last_of('/'));
    //make dir with previlege:read, write, execute/search by group,owner and other
    if(DRS_OK != mkdir_if_not_exist(path.c_str(),S_IRWXG|S_IRWXU|S_IRWXO))
        return INTERNAL_ERROR;
    // file exsits
    if(access(name.c_str(),R_OK && W_OK) == 0){
        return DRS_OK;
    }
    // create file
    FILE* file = fopen(name.c_str(), "ab+");
    if(nullptr != file){ // TODO:create sparse file
#if 0
//        fseek(file,max_journal_size_-1,SEEK_SET);
//        fputc('\0',file);
        char buf[1024] = {'\0'};
        for(int i=0;i<max_journal_size_/1024; i++){
            fwrite(buf,1,sizeof(buf),file);
        }
#endif
        fclose(file);
        return DRS_OK;
    }
    LOG_ERROR << "create journal file " << name << " failed:" << strerror(errno);
    return INTERNAL_ERROR;
}

bool CephS3Meta::_get_journal_meta(const string& key, JournalMeta& meta){
    string value;
    RESULT res = s3Api_ptr_->get_object(key.c_str(),&value);
    if(res != DRS_OK)
        return false;
    if(true != meta.ParseFromString(value)){
        LOG_ERROR << "parser journal " << key <<" 's meta failed!";
        return false;
    }
    return true;
}

bool CephS3Meta::_get_replayer_producer_marker(const string& key,
        JournalMarker& marker){
    return get_marker(key,REPLAYER,marker,false);
}
bool  CephS3Meta::_get_replayer_consumer_marker(const string& key,
        JournalMarker& marker){
    return get_marker(key,REPLAYER,marker,true);
}
bool CephS3Meta::_get_replicator_producer_marker(const string& key,
        JournalMarker& marker){
    return get_marker(key,REPLICATOR,marker,false);
}
bool CephS3Meta::_get_replicator_consumer_marker(const string& key,
        JournalMarker& marker){
    return get_marker(key,REPLICATOR,marker,true);
}
bool CephS3Meta::_get_volume_meta(const string& id,VolumeMeta& meta){
    string key = construct_volume_meta_key(id);
    string value;
    RESULT res = s3Api_ptr_->get_object(key.c_str(),&value);
    if(DRS_OK != res){
        LOG_ERROR << "get volume meta failed:" << key;
        return false;
    }
    return meta.ParseFromString(value);
}

bool CephS3Meta::get_marker(const string& vol_id,
        const CONSUMER_TYPE& type, JournalMarker& marker,bool is_consumer){
    string key = assemble_journal_marker_key(vol_id,type,is_consumer);
    string value;
    RESULT res = s3Api_ptr_->get_object(key.c_str(),&value);
    if(DRS_OK == res){
        return marker.ParseFromString(value);
    }
    else if(NO_SUCH_KEY == res && is_consumer == true){
    // if the consumer failed without init the marker yet? maybe the dr server
    // should init the marker if it's not init, or the restarted consumer may not know where to start 
        LOG_WARN << vol_id << " 's consumer marker is not initialized!";
        std::list<string> list;
        res = s3Api_ptr_->list_objects((g_key_prefix+vol_id).c_str(),nullptr,1,&list);
        if(DRS_OK != res){
            LOG_ERROR << "list volume " << vol_id << " journals failed!";
            return false;
        }
        if(list.size() <= 0){
            return false;
        }
        else{
            marker.set_cur_journal(list.front());
            marker.set_pos(0L);
            return true;
        }
    }
    return false;
}

RESULT CephS3Meta::get_journal_meta(const string& key, JournalMeta& meta) {
    if(journal_meta_cache_.get(key,meta)){
        return DRS_OK;
    }
    else
        return INTERNAL_ERROR;
}

int CephS3Meta::compare_journal_key(const string& vol_id,
                const string& key1,const string& key2){
    std::shared_ptr<JournalHeadTail> head_tail = get_journal_key_counter(vol_id);
    DR_ASSERT(head_tail != nullptr);
    uint64_t counter1;
    uint64_t counter2;
    DR_ASSERT(true == sg_util::extract_counter_from_object_key(key1,counter1));
    DR_ASSERT(true == sg_util::extract_counter_from_object_key(key2,counter2));
    if(head_tail->head < head_tail->tail){
        return counter1 > counter2 ? 1:( counter1==counter2 ? 0:-1);
    }
    else{ // tail in at the next loop
        if(counter1 < head_tail->head)
            counter1 += MAX_JOURNAL_COUNT;
        if(counter2 < head_tail->head)
            counter2 += MAX_JOURNAL_COUNT;
        return counter1 > counter2 ? 1:( counter1==counter2 ? 0:-1);
    }
}

int CephS3Meta::compare_marker(const string& vol_id,
            const JournalMarker& m1, const JournalMarker& m2){
    int res = compare_journal_key(vol_id,m1.cur_journal(),m2.cur_journal());
    if(res == 0){
        return m1.pos() > m2.pos() ? 1:(m1.pos() == m2.pos() ? 0:-1);
    }
    else {
        return res;
    }
}

std::shared_ptr<JournalHeadTail> CephS3Meta::init_journal_key_counter(
                            const string& vol_id){
    std::list<string> list;
    uint64_t counter1=0;
    string prefix = g_key_prefix + vol_id;
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,0,&list);
    if(DRS_OK != res) {
        LOG_ERROR << "list volume " << vol_id << " opened journals failed!";
        return nullptr;
    }
    if(!list.empty()){
        std::shared_ptr<JournalHeadTail> head_tail = init_head_tail_by_journals(list);
        if(nullptr == head_tail){
            LOG_ERROR << "init head_tail failed!,volume=" << vol_id;
            return nullptr;
        }
        boost::unique_lock<boost::shared_mutex> lck(counter_mtx);
        counter_map_.insert(std::pair<string,std::shared_ptr<JournalHeadTail>>(vol_id,head_tail));

        LOG_INFO << "init volume [" << vol_id << "] journal counter head:"
            << head_tail->head << ",tail:" << head_tail->tail;
        return head_tail;
    }
    else{
        LOG_INFO << "init volume " << vol_id << " journal name counter:"
            << MIN_JOURNAL_COUNTER;
        std::shared_ptr<JournalHeadTail> head_tail = std::make_shared<JournalHeadTail>();
        head_tail->head = MIN_JOURNAL_COUNTER;
        head_tail->tail = MIN_JOURNAL_COUNTER;
        boost::unique_lock<boost::shared_mutex> lck(counter_mtx);

        counter_map_.insert(std::pair<string,std::shared_ptr<JournalHeadTail>>(vol_id,head_tail));
        return head_tail;
    }    
}

std::shared_ptr<JournalHeadTail> CephS3Meta::get_journal_key_counter(
                            const string& vol_id){
    boost::shared_lock<boost::shared_mutex> read_lck(counter_mtx);
    auto it = counter_map_.find(vol_id);
    if(counter_map_.end() == it) { // volume not found
        read_lck.unlock();
        LOG_INFO << "get journal key counter:volume " << vol_id << " not found.";
        return init_journal_key_counter(vol_id);
    }
    return it->second;
}

// cephS3Meta member functions
CephS3Meta::CephS3Meta():
    journal_meta_cache_(1000,std::bind(&CephS3Meta::_get_journal_meta,
        this,std::placeholders::_1,std::placeholders::_2)),

    replayer_Pmarker_cache_(128,std::bind(
        &CephS3Meta::_get_replayer_producer_marker,
        this,std::placeholders::_1,std::placeholders::_2)),

    replicator_Pmarker_cache_(128,std::bind(
        &CephS3Meta::_get_replicator_producer_marker,
        this,std::placeholders::_1,std::placeholders::_2)),

    replayer_Cmarker_cache_(128,std::bind(
        &CephS3Meta::_get_replayer_consumer_marker,
        this,std::placeholders::_1,std::placeholders::_2)),

    replicator_Cmarker_cache_(128,std::bind(
        &CephS3Meta::_get_replicator_consumer_marker,
        this,std::placeholders::_1,std::placeholders::_2)),

    vol_cache_(128,std::bind(&CephS3Meta::_get_volume_meta,
        this,std::placeholders::_1,std::placeholders::_2)) {
    init();
}
CephS3Meta::~CephS3Meta() {
}

RESULT CephS3Meta::init() {
    string access_key;
    string secret_key;
    string host;
    string bucket_name;
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    if(false == parser->get<string>("ceph_s3.access_key",access_key)){
        LOG_FATAL << "config parse ceph_s3.access_key error!";
        return INTERNAL_ERROR;
    }
    if(false == parser->get<string>("ceph_s3.secret_key",secret_key)){
        LOG_FATAL << "config parse ceph_s3.secret_key error!";
        return INTERNAL_ERROR;
    }
    // port number is necessary if not using default 80/443
    if(false == parser->get<string>("ceph_s3.host",host)){
        LOG_FATAL << "config parse ceph_s3.host error!";
        return INTERNAL_ERROR;
    }
    if(false == parser->get<string>("ceph_s3.bucket",bucket_name)){
        LOG_FATAL << "config parse ceph_s3.bucket error!";
        return INTERNAL_ERROR;
    }
    s3Api_ptr_.reset(new CephS3Api(access_key.c_str(),
            secret_key.c_str(),host.c_str(),bucket_name.c_str()));
    string type;
    if(false == parser->get<string>("global.journal_storage",type)){
        LOG_FATAL << "config parse global.journal_storage error!";
        return INTERNAL_ERROR;
    }
    if(type.compare("ceph_fs") == 0){
        if(false == parser->get<string>("ceph_fs.mount_point",mount_path_)){
            LOG_FATAL << "config parse ceph_fs.mount_point error!";
            return INTERNAL_ERROR;
        }
    }
    else
        DR_ERROR_OCCURED();
    max_journal_size_ = parser->get_default<int>(
        "journal_writer.journal_max_size",32 * 1024 * 1024);
    return mkdir_if_not_exist((mount_path_+g_key_prefix).c_str(),S_IRWXG|S_IRWXU|S_IRWXO);
}

RESULT CephS3Meta::create_journals(const string& uuid,const string& vol_id,
        const int& limit, std::list<string>& list){

    // get last journal counter
    std::shared_ptr<JournalHeadTail> head_tail = get_journal_key_counter(vol_id);
    if(head_tail == nullptr)
        return INTERNAL_ERROR;

    LOG_INFO << vol_id << " creating journals from " 
        << head_tail->tail << ",number " << limit;

    RESULT res = DRS_OK;
    // create journals
    string journals[limit];
    for(int i=0;i<limit;i++) {
        // the lock here guarantee that only one thread meantain the journals
        // of this volume, and keep the journal counter sequential
        std::lock_guard<std::mutex> lck(head_tail->mtx);

        // head is equal with tail means empty, head==tail+1%num means full,
        // an empty slot can help to distinguish the head & tail
        if((head_tail->tail + 1)% MAX_JOURNAL_COUNT == head_tail->head){
            LOG_WARN << "volume[" << vol_id << "] journals counter used up.";
            return list.empty()? INTERNAL_ERROR:DRS_OK;
        }
        uint64_t next = head_tail->tail;        
        next = next << SUB_COUNTER_BITS;
        journals[i] = sg_util::construct_journal_key(vol_id,next);

        // create journal key
        string filename = get_journal_filename(vol_id,next);
        JournalMeta meta;
        meta.set_path(filename);
        meta.set_status(OPENED);
        string meta_s;
        if(true != meta.SerializeToString(&meta_s)){
            LOG_ERROR << "serialize journal meta failed!";
            res = INTERNAL_ERROR;
            break;
        }
        res = s3Api_ptr_->put_object(journals[i].c_str(),&meta_s,nullptr); // add journal major key
        if(DRS_OK != res){
            LOG_ERROR << "update journal " << journals[i] << " opened status failed!";
            break;
        }
        // update cache
        journal_meta_cache_.put(journals[i],meta);

        // create journal index key, which will help in GC
        string o_key = construct_write_open_index(journals[i],vol_id,uuid);
        res = s3Api_ptr_->put_object(o_key.c_str(),nullptr,nullptr);
        if(DRS_OK != res){
            LOG_ERROR << "add opened journal's index key " << o_key << " failed!";
            break;
        }
        res = create_journal_file(mount_path_ + filename);
        if(DRS_OK != res){
            LOG_ERROR << "creat file " << mount_path_+filename << " failed!" ;
            break;
        }
        list.push_back(journals[i]);
        head_tail->tail = (head_tail->tail + 1) % MAX_JOURNAL_COUNT;
    }

    if(DRS_OK != res){
        if(list.size() <= 0)
            return INTERNAL_ERROR;
        // roll back: delete partial meta
        // TODO:recycle the pending journal file
        s3Api_ptr_->delete_object(journals[list.size()].c_str());
        journal_meta_cache_.delete_key(journals[list.size()].c_str());
        string o_key = construct_write_open_index(journals[list.size()],vol_id,uuid);
        s3Api_ptr_->delete_object(o_key.c_str());
        return DRS_OK; // partial success
    }
    return res;
}

RESULT CephS3Meta::create_journals_by_given_keys(const string& uuid,
            const string& vol_id,const std::list<string> &list){
    RESULT res = DRS_OK;
    uint64_t next;
    int count = 0;
    for(auto it=list.begin();it!=list.end();++it) {
        JournalMeta meta;
        if(journal_meta_cache_.get(*it,meta))// journal existed
            continue;
        if(true != sg_util::extract_counter_from_object_key(*it,next)){
            res = INTERNAL_ERROR;
            break;
        }
        string filename = get_journal_filename(vol_id,next);
        meta.set_path(filename);
        meta.set_status(OPENED);
        string meta_s;
        if(true != meta.SerializeToString(&meta_s)){
            LOG_ERROR << "serialize journal meta failed!";
            res = INTERNAL_ERROR;
            break;
        }
        // persit journal major key
        res = s3Api_ptr_->put_object(it->c_str(),&meta_s,nullptr);
        if(DRS_OK != res){
            LOG_ERROR << "create journal " << *it << " failed!";
            break;
        }
        // update cache
        journal_meta_cache_.put(*it,meta);
        count++;

        // persit journal index key
        string o_key = construct_write_open_index(*it,vol_id,uuid);
        res = s3Api_ptr_->put_object(o_key.c_str(),nullptr,nullptr);
        if(DRS_OK != res){
            LOG_ERROR << "add opened journal's index key " << o_key << " failed!";
            break;
        }
        res = create_journal_file(mount_path_ + filename);
        if(DRS_OK != res){
            LOG_ERROR << "creat file " << mount_path_+filename << " failed!" ;
            break;
        }
    }
    if(DRS_OK != res){
        if(count <= 0)
            return INTERNAL_ERROR;
        // roll back: delete partial written meta
        string key = sg_util::construct_journal_key(vol_id,next);
        string o_key = construct_write_open_index(key,vol_id,uuid);
        // TODO:recycle the pending journal file
        s3Api_ptr_->delete_object(o_key.c_str());
        s3Api_ptr_->delete_object(key.c_str());
        journal_meta_cache_.delete_key(key);
        return INTERNAL_ERROR;
    }
    return res;
}

RESULT CephS3Meta::seal_volume_journals(const string& uuid, const string& vol_id,
        const string journals[], const int& count) {
    RESULT res = DRS_OK;    
    for(int i=0;i<count;i++){
        JournalMeta meta;
        res = get_journal_meta(journals[i],meta);
        if(DRS_OK != res){
            LOG_ERROR << "get journal " << journals[i] << " meta failed!";
            break;
        }
        meta.set_status(SEALED);
        string meta_s;
        if(true != meta.SerializeToString(&meta_s)){
            LOG_ERROR << "serialize journal meta failed!";
            res = INTERNAL_ERROR;
            break;
        }
        res = s3Api_ptr_->put_object(journals[i].c_str(),&meta_s,nullptr); // modify journal major key
        if(DRS_OK != res){
            LOG_ERROR << "update journal " << journals[i] << " sealed status failed!";
            break;
        }
        journal_meta_cache_.put(journals[i],meta); // update cache
        string s_key = construct_sealed_index(journals[i]);
        res = s3Api_ptr_->put_object(s_key.c_str(),nullptr,nullptr); // add journal sealed index key
        if(DRS_OK != res){
            LOG_ERROR << "add sealed journal's index key " << s_key << " failed!";
            break;
        }
        string o_key = construct_write_open_index(journals[i],vol_id,uuid);
        res = s3Api_ptr_->delete_object(o_key.c_str());
        if(DRS_OK != res){
            LOG_ERROR << "delete opened journal's index key " << o_key << " failed!";
            // no break, continue trying to delete in GC thread
        }
    }
    // TODO: sg_client invoke this process
    // set producer marker if it's ahead of the last sealed journal
    /*
    VolumeMeta meta;
    res = read_volume_meta(vol_id,meta);
    DR_ASSERT(DRS_OK == res);
    if(REP_PRIMARY == meta.info().role()){
        JournalMarker marker;
        marker.set_cur_journal(journals[count-1]);
        marker.set_pos(max_journal_size_);
        set_producer_marker(vol_id,marker);
    }
    */
    return res;
}

RESULT CephS3Meta::get_consumer_marker(const string& vol_id,
        const CONSUMER_TYPE& type, JournalMarker& marker){
    switch(type){
        case REPLAYER:
            if(!replayer_Cmarker_cache_.get(vol_id,marker))
                return INTERNAL_ERROR;
            break;
        case REPLICATOR:
            if(!replicator_Cmarker_cache_.get(vol_id,marker))
                return INTERNAL_ERROR;
            break;
        default:
            return INTERNAL_ERROR;
    }

    LOG_INFO << "get consumer marker:" << vol_id << ",type:" << type 
        << "\n " << marker.cur_journal() << ":" << marker.pos();
    return DRS_OK;
}

RESULT CephS3Meta::update_consumer_marker(const string& vol_id,
        const CONSUMER_TYPE& type,const JournalMarker& marker){
    string key = assemble_journal_marker_key(vol_id,type,true);
    string marker_s;
    if(false==marker.SerializeToString(&marker_s)){
        LOG_ERROR << vol_id << " serialize marker failed!";
        return INTERNAL_ERROR;
    }
    RESULT res = s3Api_ptr_->put_object(key.c_str(),&marker_s,nullptr);
    if(DRS_OK != res){
        LOG_ERROR << "update_journal_marker of volume " << vol_id << " failed!";
        return res;
    }
    if(REPLAYER == type){
        replayer_Cmarker_cache_.put(vol_id,marker);
    }
    else if(REPLICATOR == type){
        replicator_Cmarker_cache_.put(vol_id,marker);
    }

    return DRS_OK;
}

RESULT CephS3Meta::set_producer_marker(const string& vol_id,
        const JournalMarker& marker){
    LOG_DEBUG << "set producer marker" << marker.cur_journal()
        << ":" << marker.pos();
    // if new marker is ahead of the old marker, drop it
    JournalMarker old_marker;
    if(replayer_Pmarker_cache_.get(vol_id,old_marker)){
        int c = old_marker.cur_journal().compare(marker.cur_journal());
        if(c > 0 || (c==0 && old_marker.pos() >= marker.pos())){
            LOG_WARN << "new producer marker is ahead of old marker:"
                << old_marker.cur_journal();
            return DRS_OK;
        }
    }

    string marker_s;
    if(false==marker.SerializeToString(&marker_s)){
        LOG_ERROR << vol_id << " serialize marker failed!";
        return INTERNAL_ERROR;
    }
    // update replayer producer marker
    string key = assemble_journal_marker_key(vol_id,REPLAYER,false);
    RESULT res = s3Api_ptr_->put_object(key.c_str(),&marker_s,nullptr);
    if(DRS_OK != res){
        LOG_ERROR << "set replayer producer marker of volume "
            << vol_id << " failed!";
        return res;
    }
    replayer_Pmarker_cache_.put(vol_id,marker);

    // update replicator producer marker, but not in some cases
    VolumeMeta meta;
    res = read_volume_meta(vol_id,meta);
    DR_ASSERT(DRS_OK == res);
    if(REP_ENABLED == meta.info().rep_status()
        && REP_PRIMARY == meta.info().role()){
        key = assemble_journal_marker_key(vol_id,REPLICATOR,false);
        RESULT res = s3Api_ptr_->put_object(key.c_str(),&marker_s,nullptr);
        if(DRS_OK != res){
            LOG_ERROR << "set replicator producer marker of volume "
                << vol_id << " failed!";
            return res;
        }
        replicator_Pmarker_cache_.put(vol_id,marker);
    }
    return DRS_OK;
}

RESULT CephS3Meta::get_producer_marker(const string& vol_id,
        const CONSUMER_TYPE& type, JournalMarker& marker){
    switch(type){
        case REPLAYER:
            if(replayer_Pmarker_cache_.get(vol_id,marker))
                return DRS_OK;
            break;
        case REPLICATOR:
            if(replicator_Pmarker_cache_.get(vol_id,marker))
                return DRS_OK;
            break;
        default:
            break;
    }
    return INTERNAL_ERROR;
}

RESULT CephS3Meta::get_consumable_journals(const string& vol_id,
        const JournalMarker& marker, const int& limit,
        std::list<JournalElement> &list,
        const CONSUMER_TYPE& type) {
    if(!marker.IsInitialized()) {
        LOG_ERROR << vol_id << " 's marker is not initialized!";
        return INTERNAL_ERROR;
    }
    char* end_marker = nullptr;
    JournalMarker producer_marker;
    RESULT res = get_producer_marker(vol_id,type,producer_marker);
    if(DRS_OK != res){
        LOG_ERROR << "get producer marker failed:" << vol_id;
        return res;
    }
    else{
        end_marker = const_cast<char*>(producer_marker.cur_journal().c_str());
        LOG_INFO << "producer marker,type=" << type << ":" << end_marker
            << ":" << producer_marker.pos();
    }
    if(marker.cur_journal().compare(producer_marker.cur_journal()) == 0){
        DR_ASSERT(marker.pos() <= producer_marker.pos());
        if(marker.pos() == producer_marker.pos())
            return DRS_OK;
        JournalElement e;
        e.set_journal(marker.cur_journal());
        e.set_start_offset(marker.pos());
        e.set_end_offset(producer_marker.pos());
        list.push_back(e);
    }
    else{
        const char* marker_key = marker.cur_journal().c_str();
        string prefix = g_key_prefix+vol_id;
        std::list<string> journal_list;
        res = s3Api_ptr_->list_objects(prefix.c_str(),marker_key,
                0,&journal_list);
        if(DRS_OK != res){
            LOG_ERROR << "list volume " << vol_id << " journals failed!";
            return INTERNAL_ERROR;
        }

        sort_journals(vol_id,journal_list);

        if(marker.pos() < max_journal_size_){
            JournalElement beg;
            beg.set_journal(marker.cur_journal());
            beg.set_start_offset(marker.pos());
            beg.set_end_offset(max_journal_size_);
            list.push_back(beg);
            LOG_DEBUG << "get consumable journal 1:" << marker.cur_journal()
                << "," << beg.start_offset() << ":" << beg.end_offset();
        }
        else{
            LOG_INFO << "journal consumed to EOF:" << marker.cur_journal();
        }
        for(string key:journal_list){
            if(compare_journal_key(vol_id,key,producer_marker.cur_journal()) >= 0)
                break;
            JournalElement e;
            e.set_journal(key);
            e.set_start_offset(0);
            e.set_end_offset(max_journal_size_);
            list.push_back(e);
            LOG_DEBUG << "get consumable journal:" << key;
            // if have enough journals,return
            if(list.size() >= limit)
                return DRS_OK;
        }
        // add the last consumer journal
        if(producer_marker.pos() > 0){
            JournalElement end;
            end.set_journal(producer_marker.cur_journal());
            end.set_start_offset(0);
            end.set_end_offset(producer_marker.pos());
            list.push_back(end);
            LOG_DEBUG << "get consumable journal end:"
                << producer_marker.cur_journal()
                << "," << end.start_offset() << ":" << end.end_offset();
        }
        else{
            LOG_INFO << "producer at begging of journal:"
                << producer_marker.cur_journal();
        }
    }
    return DRS_OK;
}

// gc manager
RESULT CephS3Meta::get_sealed_and_consumed_journals(
        const string& vol_id, const JournalMarker& marker,const int& limit,
        std::list<string> &list) {
    LOG_DEBUG << "get sealed&consumed journals from " << marker.cur_journal();
    // list sealed journals
    std::list<string> sealed_journals;
    string prefix = g_sealed+vol_id;  // recycle sealed journals
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,
        limit,&sealed_journals);
    if(res != DRS_OK){
        return res;
    }
    for(auto it=sealed_journals.begin();it!=sealed_journals.end();++it){
        uint64_t counter;
        sg_util::extract_counter_from_object_key(*it,counter);
        string key = sg_util::construct_journal_key(vol_id,counter);
        if(compare_journal_key(vol_id,key,marker.cur_journal()) < 0)
            list.push_back(key);
        else
            break;
    }
    return DRS_OK;
}

RESULT CephS3Meta::recycle_journals(const string& vol_id,
        const std::list<string>& journals){
    RESULT res;
    for(auto it=journals.begin();it!=journals.end();it++){
        string value;
        res = s3Api_ptr_->get_object(it->c_str(),&value);
        if(res != DRS_OK){
            LOG_WARN << " recycle journal " << *it << " failed.";
            continue;
        }
        string r_key = construct_recyled_index(*it);
        res = s3Api_ptr_->put_object(r_key.c_str(),&value,nullptr);
        DR_ASSERT(DRS_OK == res);
        string s_key = construct_sealed_index(*it);
        s3Api_ptr_->delete_object(s_key.c_str());
        res = s3Api_ptr_->delete_object(it->c_str());
        DR_ASSERT(DRS_OK == res);
        // delete cached keys
        journal_meta_cache_.delete_key(*it);
    }
    // recycle journal counter
    std::shared_ptr<JournalHeadTail> head_tail = get_journal_key_counter(vol_id);
    DR_ASSERT(head_tail != nullptr);
    std::lock_guard<std::mutex> lck(head_tail->mtx);
    head_tail->head = (head_tail->head + journals.size()) % MAX_JOURNAL_COUNT;
    LOG_INFO << "update volume[" << vol_id << "] journal counter head at "
        << head_tail->head;
    return res;
}

RESULT CephS3Meta::get_producer_id(const string& vol_id,
        std::list<string>& list){
    string prefix = g_writer_prefix+vol_id+"/";
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,0,&list,"/");
    if(DRS_OK != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return res;
    }
    if(list.empty())
        return DRS_OK;
    std::for_each(list.begin(),list.end(),[](string& s){
        // get "/uuid/" in "/sessions/writer/vol/uuid/"
        size_t end = s.find_last_of('/');
        size_t pos = s.find_last_of('/',end-1);
        s = s.substr(pos+1,end-pos-1);// remove '/' at start and end
    });
    return DRS_OK;
}
RESULT CephS3Meta::seal_opened_journals(const string& vol_id,
        const string& uuid){
    std::list<string> list;
    string prefix = g_writer_prefix+vol_id+"/"+uuid+"/";
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,0,&list);
    if(DRS_OK != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return res;
    }
    if(list.empty())
        return DRS_OK;
    std::for_each(list.begin(),list.end(),[=](string& s){
        //replace "/sessions/writer/vol-id/uuid/" with "/journals/vol-id/"
        size_t pos = s.find_last_of('/');
        s = s.substr(pos+1);
        s = g_key_prefix + vol_id + "/" + s;
    });
    string keys_a[list.size()];
    std::copy(list.begin(),list.end(),keys_a);
    return seal_volume_journals(uuid,vol_id,keys_a,list.size());
}
RESULT CephS3Meta::list_volumes(std::list<string>& list){
    string prefix = g_key_prefix;
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,0,&list,"/");
    if(DRS_OK != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return res;
    }
    if(list.empty())
        return DRS_OK;
    std::for_each(list.begin(),list.end(),[](string& s){
        // get "vol-id" in "/journals/vol-id/"
        size_t end = s.find_last_of('/');
        size_t pos = s.find_last_of('/',end-1);
        s = s.substr(pos+1,end-pos-1);// remove '/' at start and end
    });
    return DRS_OK;
}

RESULT CephS3Meta::list_volume_meta(std::list<VolumeMeta> &list){
    std::list<string> reps;
    RESULT res = s3Api_ptr_->list_objects(g_volume_prefix.c_str(),
        nullptr,0,&reps,"/");
    if(DRS_OK != res){
        return INTERNAL_ERROR;
    }
    for(string rep:reps){
        string key = rep.substr(g_volume_prefix.length());//extract vol id
        VolumeMeta meta;
        if(DRS_OK != read_volume_meta(key,meta)){
             LOG_ERROR << "get " << key << " failed!";
            return INTERNAL_ERROR;
        }
        list.push_back(meta);
    }
    return DRS_OK;
}
RESULT CephS3Meta::read_volume_meta(const string& id,VolumeMeta& meta){
    if(vol_cache_.get(id,meta))
        return DRS_OK;
    else
        return INTERNAL_ERROR;
}
RESULT CephS3Meta::create_volume(const VolumeMeta& meta){
    string key = construct_volume_meta_key(meta.info().vol_id());
    string value;
    if(false == meta.SerializeToString(&value)){
        LOG_ERROR << "serailize replication meta failed!";
        return INTERNAL_ERROR;
    }
    if(DRS_OK == s3Api_ptr_->put_object(key.c_str(),&value,nullptr)){
        vol_cache_.put(meta.info().vol_id(),meta);
        return DRS_OK;
    }
    return INTERNAL_ERROR;
}
RESULT CephS3Meta::update_volume_meta(const VolumeMeta& meta){
    string key = construct_volume_meta_key(meta.info().vol_id());
    if(DRS_OK != s3Api_ptr_->head_object(key.c_str(),nullptr)){
        LOG_ERROR << "retrieve volume meta failed:" << key;
        return INTERNAL_ERROR;
    }
    else
        return create_volume(meta);
}
RESULT CephS3Meta::delete_volume(const string& id){
    string key = construct_volume_meta_key(id);
    vol_cache_.delete_key(id);
    return  s3Api_ptr_->delete_object(key.c_str());
}

