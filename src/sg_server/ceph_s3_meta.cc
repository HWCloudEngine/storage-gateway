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
#include <chrono>
#include "ceph_s3_meta.h"
#include "log/log.h"
#include "common/config_parser.h"
#include "sg_util.h"
// TODO:config, alert if more than MAX_JOURNAL_COUNT journals not consumed
const uint64_t MAX_JOURNAL_COUNT = 10000LU;
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
static string get_journal_filename(const string& vol_id,const string& key){
    string counter_str = key.substr(key.find_last_of("/") + 1,string::npos);
    return g_key_prefix + vol_id + "/" + counter_str;
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

string get_journal_key_by_index_key(const string& index){
    string sub = index;
    sub.erase(0,sub.find_first_of("/")+1);
    sub.erase(0,sub.find_first_of("/")+1);
    return g_key_prefix + sub;
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
    StatusCode res = kvApi_ptr_->get_object(key.c_str(),&value);
    if(StatusCode::sOk != res)
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
    StatusCode res = kvApi_ptr_->get_object(key.c_str(),&value);
    if(StatusCode::sOk != res){
        LOG_ERROR << "get volume meta failed:" << key;
        return false;
    }
    if(meta.ParseFromString(value)){
        return true;
    }
    else {
        LOG_ERROR << "deserialize volume meta failed:" << id;
        return false;
    }
}

bool CephS3Meta::get_marker(const string& vol_id,
        const CONSUMER_TYPE& type, JournalMarker& marker,bool is_consumer){
    string key = assemble_journal_marker_key(vol_id,type,is_consumer);
    string value;
    StatusCode res = kvApi_ptr_->get_object(key.c_str(),&value);
    if(StatusCode::sOk == res){
        return marker.ParseFromString(value);
    }
    else if(StatusCode::sNotFound == res && is_consumer == true){
    // if the consumer failed without init the marker yet? maybe the dr server
    // should init the marker if it's not init, or the restarted consumer may not know where to start 
        LOG_WARN << vol_id << " 's consumer marker is not initialized!";
        std::list<string> list;
        res = kvApi_ptr_->list_objects((g_key_prefix+vol_id).c_str(),nullptr,1,&list);
        if(StatusCode::sOk != res){
            LOG_ERROR << "list volume " << vol_id << " journals failed!";
            return false;
        }
        if(list.size() <= 0){
            return false;
        }
        else{
            // TODO: how to init for secondary site
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

int CephS3Meta::compare_journal_key(const string& key1,
        const string& key2){
    return key1.compare(key2);
}

int CephS3Meta::compare_marker(const JournalMarker& m1,
        const JournalMarker& m2){
    int res = compare_journal_key(m1.cur_journal(),m2.cur_journal());
    if(res == 0){
        return m1.pos() > m2.pos() ? 1:(m1.pos() == m2.pos() ? 0:-1);
    }
    else {
        return res;
    }
}

std::shared_ptr<JournalCounter> CephS3Meta::init_journal_key_counter(
                            const string& vol_id){
    std::list<string> list;
    uint64_t counter1=0;
    string prefix = g_key_prefix + vol_id;
    StatusCode res = kvApi_ptr_->list_objects(prefix.c_str(),nullptr,0,&list);
    if(StatusCode::sOk != res) {
        LOG_ERROR << "list volume " << vol_id << " opened journals failed!";
        return nullptr;
    }
    std::shared_ptr<JournalCounter> journal_counter =
            std::make_shared<JournalCounter>();
    if(!list.empty()){
        SG_ASSERT(true == sg_util::extract_major_counter_from_journal_key(
                list.back(),journal_counter->next));
        WriteLock lck(counter_mtx);
        journal_counter->next ++;
        counter_map_.insert(std::pair<string,std::shared_ptr<JournalCounter>>(vol_id,journal_counter));

        LOG_INFO << "init volume [" << vol_id << "] journal counter :" << std::hex
            << journal_counter->next << std::dec;
        return journal_counter;
    }
    else{
        LOG_INFO << "init volume " << vol_id << " journal name counter:"
            << MIN_JOURNAL_COUNTER;
        journal_counter->next = MIN_JOURNAL_COUNTER;
        WriteLock lck(counter_mtx);
        counter_map_.insert(std::pair<string,std::shared_ptr<JournalCounter>>(vol_id,journal_counter));
        return journal_counter;
    }    
}

std::shared_ptr<JournalCounter> CephS3Meta::get_journal_key_counter(
                            const string& vol_id){
    ReadLock read_lck(counter_mtx);
    auto it = counter_map_.find(vol_id);
    if(counter_map_.end() == it) { // volume not found
        read_lck.unlock();
        LOG_INFO << "get journal key counter:volume " << vol_id << " not found.";
        return init_journal_key_counter(vol_id);
    }
    return it->second;
}

// cephS3Meta member functions
CephS3Meta::CephS3Meta(std::shared_ptr<KVApi> kvApi_ptr):
    kvApi_ptr_(kvApi_ptr),
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
    mount_path_ = g_option.journal_mount_point;
    max_journal_size_ = g_option.journal_max_size;
    
    return mkdir_if_not_exist((mount_path_+g_key_prefix).c_str(),S_IRWXG|S_IRWXU|S_IRWXO);
}

RESULT CephS3Meta::create_journals(const string& uuid,const string& vol_id,
        const int& limit, std::list<JournalElement>& list){

    // get last journal counter
    std::shared_ptr<JournalCounter> journal_counter = get_journal_key_counter(vol_id);
    if(journal_counter == nullptr)
        return INTERNAL_ERROR;

    LOG_INFO << vol_id << " creating journals from " << std::hex
        << journal_counter->next << std::dec << ",number " << limit;

    RESULT res = DRS_OK;
    // create journals
    string journals[limit];
    for(int i=0;i<limit;i++) {
        // the lock here guarantee that only one thread meantain the journals
        // of this volume, and keep the journal counter sequential
        std::lock_guard<std::mutex> lck(journal_counter->mtx);

        // TODO: if un-consumed journals increases abnormally, disable?

        uint64_t next = journal_counter->next;
        journals[i] = sg_util::construct_journal_key(vol_id,next);

        // create journal key
        string filename = get_journal_filename(vol_id,journals[i]);
        JournalMeta meta;
        meta.set_path(filename);
        meta.set_status(OPENED);
        string meta_s;
        if(true != meta.SerializeToString(&meta_s)){
            LOG_ERROR << "serialize journal meta failed!";
            res = INTERNAL_ERROR;
            break;
        }
        // add journal major key
        StatusCode ret = kvApi_ptr_->put_object(journals[i].c_str(),&meta_s,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "update journal " << journals[i] << " opened status failed!";
            res = INTERNAL_ERROR;
            break;
        }

        // create journal index key, which will help in GC
        string o_key = construct_write_open_index(journals[i],vol_id,uuid);
        ret = kvApi_ptr_->put_object(o_key.c_str(),nullptr,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "add opened journal's index key " << o_key << " failed!";
            res = INTERNAL_ERROR;
            break;
        }
        res = create_journal_file(mount_path_ + filename);
        if(DRS_OK != res){
            LOG_ERROR << "creat file " << mount_path_+filename << " failed!" ;
            break;
        }
        JournalElement e;
        e.set_journal(journals[i]);
        e.set_path(meta.path());
        e.set_status(meta.status());
        e.set_start_offset(0);
        e.set_end_offset(max_journal_size_);
        list.push_back(e);
        journal_counter->next ++;
        // update cache
        journal_meta_cache_.put(journals[i],meta);
    }

    if(DRS_OK != res){
        if(list.size() <= 0)
            return INTERNAL_ERROR;
        // roll back: delete partial meta
        // TODO:recycle the pending journal file
        kvApi_ptr_->delete_object(journals[list.size()].c_str());
        string o_key = construct_write_open_index(journals[list.size()],vol_id,uuid);
        kvApi_ptr_->delete_object(o_key.c_str());
        return DRS_OK; // partial success
    }
    return res;
}

RESULT CephS3Meta::create_journals_by_given_keys(const string& uuid,
            const string& vol_id,const std::list<string> &list){
    // get counter mutex, since that there may be 2 or more thread create journals concurrently
    std::shared_ptr<JournalCounter> journal_counter = get_journal_key_counter(vol_id);
    if(journal_counter == nullptr)
        return INTERNAL_ERROR;    
    std::lock_guard<std::mutex> lck(journal_counter->mtx);

    RESULT res = DRS_OK;
    int count = 0;
    for(auto it=list.begin();it!=list.end();++it) {
        JournalMeta meta;
        if(journal_meta_cache_.get(*it,meta))// journal existed
            continue;

        string filename = get_journal_filename(vol_id,*it);
        meta.set_path(filename);
        meta.set_status(OPENED);
        string meta_s;
        if(true != meta.SerializeToString(&meta_s)){
            LOG_ERROR << "serialize journal meta failed!";
            res = INTERNAL_ERROR;
            break;
        }

        count++;
        // persit journal major key
        StatusCode ret = kvApi_ptr_->put_object(it->c_str(),&meta_s,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "create journal " << *it << " failed!";
            res = INTERNAL_ERROR;
            break;
        }

        // persit journal index key
        string o_key = construct_write_open_index(*it,vol_id,uuid);
        ret = kvApi_ptr_->put_object(o_key.c_str(),nullptr,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "add opened journal's index key " << o_key << " failed!";
            res = INTERNAL_ERROR;
            break;
        }
        res = create_journal_file(mount_path_ + filename);
        if(DRS_OK != res){
            LOG_ERROR << "creat file " << mount_path_+filename << " failed!" ;
            break;
        }
        // update journal counter;if failover, the counter should be in sequence
        uint64_t m_cnt;
        SG_ASSERT(true == sg_util::extract_major_counter_from_journal_key(*it,m_cnt));
        journal_counter->next = journal_counter->next > m_cnt+1 ?
            journal_counter->next : m_cnt+1;
        // update cache
        journal_meta_cache_.put(*it,meta);
    }
    if(DRS_OK != res){
        if(count <= 1)
            return INTERNAL_ERROR;
        // roll back: delete partial written meta
        auto it = list.begin();
        std::advance(it,count - 1);
        string key = *it;
        string o_key = construct_write_open_index(key,vol_id,uuid);
        // TODO:recycle the pending journal file
        kvApi_ptr_->delete_object(o_key.c_str());
        kvApi_ptr_->delete_object(key.c_str());
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
        // modify journal status to sealed
        StatusCode ret = kvApi_ptr_->put_object(journals[i].c_str(),&meta_s,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "update journal " << journals[i] << " sealed status failed!";
            res = INTERNAL_ERROR;
            break;
        }
        journal_meta_cache_.put(journals[i],meta); // update cache
        string s_key = construct_sealed_index(journals[i]);
         // add journal sealed index key
        ret = kvApi_ptr_->put_object(s_key.c_str(),nullptr,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "add sealed journal's index key " << s_key << " failed!";
            res = INTERNAL_ERROR;
            break;
        }
        string o_key = construct_write_open_index(journals[i],vol_id,uuid);
        ret = kvApi_ptr_->delete_object(o_key.c_str());
        if(StatusCode::sOk != ret){
            LOG_ERROR << "delete opened journal's index key " << o_key << " failed!";
            res = INTERNAL_ERROR;
            // no break, continue trying to delete in GC thread
        }
    }

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
    StatusCode res = kvApi_ptr_->put_object(key.c_str(),&marker_s,nullptr);
    if(StatusCode::sOk != res){
        LOG_ERROR << "update_journal_marker of volume " << vol_id << " failed!";
        return INTERNAL_ERROR;
    }
    if(REPLAYER == type){
        replayer_Cmarker_cache_.put(vol_id,marker);
    }
    else if(REPLICATOR == type){
        replicator_Cmarker_cache_.put(vol_id,marker);
    }

    return DRS_OK;
}

bool CephS3Meta::wait_for_replicator_producer_maker_changed(
        int timeout){
    std::unique_lock<std::mutex> lck(p_mtx_);
    if(p_cv_.wait_for(lck,std::chrono::milliseconds(timeout)) == 
        std::cv_status::timeout) {
        return false;
    }
    else {
        return true;
    }
}

RESULT CephS3Meta::set_producer_marker(const string& vol_id,
        const JournalMarker& marker){
    LOG_DEBUG << "try set producer marker " << marker.cur_journal()
        << ":" << marker.pos();
    // if new marker is ahead of the old marker, drop it
    JournalMarker old_marker;
    if(replayer_Pmarker_cache_.get(vol_id,old_marker)){
        int c = old_marker.cur_journal().compare(marker.cur_journal());
        if(c > 0 || (c==0 && old_marker.pos() >= marker.pos())){
            LOG_WARN << "new producer marker is ahead of old marker:"
                << old_marker.cur_journal() << ":" << old_marker.pos();
            return DRS_OK;
        }
    }

    string marker_s;
    if(false==marker.SerializeToString(&marker_s)){
        LOG_ERROR << vol_id << " serialize marker failed!";
        return INTERNAL_ERROR;
    }
    // update replayer producer marker, secondary site use
    string key = assemble_journal_marker_key(vol_id,REPLAYER,false);
    StatusCode ret = kvApi_ptr_->put_object(key.c_str(),&marker_s,nullptr);
    if(StatusCode::sOk != ret){
        LOG_ERROR << "set replayer producer marker of volume "
            << vol_id << " failed!";
        return INTERNAL_ERROR;
    }
    replayer_Pmarker_cache_.put(vol_id,marker);

    // update replicator producer marker, but not in some cases
    VolumeMeta meta;
    RESULT res = read_volume_meta(vol_id,meta);
    SG_ASSERT(DRS_OK == res);
    if(REP_PRIMARY == meta.info().role()
        && REP_ENABLED == meta.info().rep_status()){
        key = assemble_journal_marker_key(vol_id,REPLICATOR,false);
        ret = kvApi_ptr_->put_object(key.c_str(),&marker_s,nullptr);
        if(StatusCode::sOk != ret){
            LOG_ERROR << "set replicator producer marker of volume "
                << vol_id << " failed!";
            return INTERNAL_ERROR;
        }
        replicator_Pmarker_cache_.put(vol_id,marker);
        LOG_INFO << "set replicator producer marker " << marker.cur_journal()
            << ":" << marker.pos();
        // trigger rep_scheduler start replicate
        std::lock_guard<std::mutex> lck(p_mtx_);
        p_cv_.notify_all();
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

    // for replayer at primary role, shoule return all journals, ignore the producer marker
    VolumeMeta meta;
    RESULT res = read_volume_meta(vol_id,meta);
    if(DRS_OK != res){
        LOG_ERROR << "get volume[" << vol_id << "] meta failed!";
        return INTERNAL_ERROR;
    }
    bool ignore_producer_marker = false;
    JournalMarker producer_marker;
    if(REPLAYER == type && REP_PRIMARY == meta.info().role()){
        ignore_producer_marker = true;
    }
    else{
        res = get_producer_marker(vol_id,type,producer_marker);
        if(DRS_OK != res){
            LOG_ERROR << "get producer marker failed:" << vol_id;
            return res;
        }
    }

    // add consumer marker to consumable list
    if(marker.pos() < max_journal_size_){
        JournalElement beg;        
        beg.set_journal(marker.cur_journal());
        beg.set_start_offset(marker.pos());
        JournalMeta m;
        if(get_journal_meta(marker.cur_journal(),m) != DRS_OK){
            LOG_ERROR << "get journal meta failed:" << marker.cur_journal();
            return INTERNAL_ERROR;
        }
        beg.set_status(m.status());
        beg.set_path(m.path());
        if(false == ignore_producer_marker 
            && compare_journal_key(marker.cur_journal(),producer_marker.cur_journal()) == 0){
            SG_ASSERT(marker.pos() <= producer_marker.pos());
            if(marker.pos() < producer_marker.pos()){
                beg.set_end_offset(producer_marker.pos());
                list.push_back(beg);
                LOG_DEBUG << "get consumable journal:" << marker.cur_journal()
                    << "," << beg.start_offset() << ":" << beg.end_offset();
            }
            return DRS_OK;
        }
        else{
            beg.set_end_offset(max_journal_size_);
            list.push_back(beg);
            LOG_DEBUG << "get consumable journal 1:" << marker.cur_journal()
                << "," << beg.start_offset() << ":" << beg.end_offset();
        }
    }
    else{ // journal was consumed to end of file, ignore
        LOG_INFO << "journal consumed to EOF:" << marker.cur_journal();
    }

    // get journal list, for replicator, if consumer marker and producmer marker
    // in the same journal, not need to get this list
    const char* marker_key = marker.cur_journal().c_str();
    string prefix = g_key_prefix+vol_id;
    std::list<string> journal_list;
    StatusCode ret = kvApi_ptr_->list_objects(prefix.c_str(),marker_key,
            0,&journal_list);
    if(StatusCode::sOk != ret){
        LOG_ERROR << "list volume " << vol_id << " journals failed!";
        return INTERNAL_ERROR;
    }
    // add journals
    for(string& key:journal_list){
        if(false == ignore_producer_marker
            && compare_journal_key(key,producer_marker.cur_journal()) >= 0)
            break;
        JournalElement e;
        e.set_journal(key);
        e.set_start_offset(0);
        e.set_end_offset(max_journal_size_);
        JournalMeta m;
        if(get_journal_meta(key,m) != DRS_OK){
            LOG_ERROR << "get journal meta failed:" << key;
            return INTERNAL_ERROR;
        }
        e.set_status(m.status());
        e.set_path(m.path());
        list.push_back(e);
        LOG_DEBUG << "get consumable journal @:" << key;
        // if have enough journals,return
        if(list.size() >= limit)
            return DRS_OK;
    }
    // add the last consumer journal, producer marker
    if(false == ignore_producer_marker && producer_marker.pos() > 0){
        JournalElement end;
        end.set_journal(producer_marker.cur_journal());
        end.set_start_offset(0);
        end.set_end_offset(producer_marker.pos());
        JournalMeta m;
        if(get_journal_meta(producer_marker.cur_journal(),m) != DRS_OK){
            LOG_ERROR << "get journal meta failed:" << producer_marker.cur_journal();
            return INTERNAL_ERROR;
        }
        end.set_status(m.status());
        end.set_path(m.path());
        list.push_back(end);
        LOG_DEBUG << "get consumable journal last:"
            << producer_marker.cur_journal()
            << "," << end.start_offset() << ":" << end.end_offset();
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
    StatusCode res = kvApi_ptr_->list_objects(prefix.c_str(),nullptr,
        limit,&sealed_journals);
    if(StatusCode::sOk != res){
        return INTERNAL_ERROR;
    }
    for(auto it=sealed_journals.begin();it!=sealed_journals.end();++it){
        string key = get_journal_key_by_index_key(*it);
        if(compare_journal_key(key,marker.cur_journal()) < 0)
            list.push_back(key);
        else
            break;
    }
    return DRS_OK;
}

RESULT CephS3Meta::recycle_journals(const string& vol_id,
        const std::list<string>& journals){
    RESULT res = DRS_OK;
    for(auto it=journals.begin();it!=journals.end();it++){
        string value;
        StatusCode ret = kvApi_ptr_->get_object(it->c_str(),&value);
        if(StatusCode::sOk != ret){
            LOG_WARN << " recycle journal " << *it << " failed.";
            res = INTERNAL_ERROR;
            continue;
        }
        string r_key = construct_recyled_index(*it);
        ret = kvApi_ptr_->put_object(r_key.c_str(),&value,nullptr);
        SG_ASSERT(StatusCode::sOk == ret);
        string s_key = construct_sealed_index(*it);
        kvApi_ptr_->delete_object(s_key.c_str());
        ret = kvApi_ptr_->delete_object(it->c_str());
        SG_ASSERT(StatusCode::sOk == ret);
        // delete cached keys
        journal_meta_cache_.delete_key(*it);
    }
    return res;
}

RESULT CephS3Meta::get_producer_id(const string& vol_id,
        std::list<string>& list){
    string prefix = g_writer_prefix+vol_id+"/";
    StatusCode res = kvApi_ptr_->list_objects(prefix.c_str(),nullptr,0,&list,"/");
    if(StatusCode::sOk != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return INTERNAL_ERROR;
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
    StatusCode res = kvApi_ptr_->list_objects(prefix.c_str(),nullptr,0,&list);
    if(StatusCode::sOk != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return INTERNAL_ERROR;
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
    StatusCode res = kvApi_ptr_->list_objects(prefix.c_str(),nullptr,0,&list,"/");
    if(StatusCode::sOk != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return INTERNAL_ERROR;
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
    StatusCode res = kvApi_ptr_->list_objects(g_volume_prefix.c_str(),
        nullptr,0,&reps,"/");
    if(StatusCode::sOk != res){
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
    if(StatusCode::sOk == kvApi_ptr_->put_object(key.c_str(),&value,nullptr)){
        vol_cache_.put(meta.info().vol_id(),meta);
        return DRS_OK;
    }
    return INTERNAL_ERROR;
}
RESULT CephS3Meta::update_volume_meta(const VolumeMeta& meta){
    string key = construct_volume_meta_key(meta.info().vol_id());
    if(StatusCode::sOk != kvApi_ptr_->head_object(key.c_str(),nullptr)){
        LOG_ERROR << "retrieve volume meta failed:" << key;
        return INTERNAL_ERROR;
    }
    else
        return create_volume(meta);
}
RESULT CephS3Meta::delete_volume(const string& id){
    string key = construct_volume_meta_key(id);
    vol_cache_.delete_key(id);
    return  (StatusCode::sOk == kvApi_ptr_->delete_object(key.c_str()))?
        DRS_OK:INTERNAL_ERROR;
}

