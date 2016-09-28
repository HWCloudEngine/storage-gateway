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
#include <unistd.h>
#include <memory>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include "ceph_s3_meta.h"
#include "log/log.h"
#include "common/config_parser.h"
#define MAX_JOURNAL_COUNTER (1000000000000L)
#define MIN_JOURNAL_COUNTER 0
const string g_key_prefix = "/journals/";
const string g_marker_prefix = "/markers/";
const string g_writer_prefix = "/session/writer/";
const string g_sealed = "/sealed/";
const string g_replayer = "replayer";
const string g_replicator = "replicator";
const string g_recycled = "/recycled/";
using std::unique_ptr;

static string counter_to_string(const int64_t& counter) {
    char tmp[13] = {0};
    std::sprintf(tmp,"%012ld",counter);
    string counter_s(tmp);
    return counter_s;
}

static string assemble_journal_marker_key(const string& vol_id,
        const CONSUMER_TYPE& type) {
    string key;
    if(REPLAYER == type){
        key += g_marker_prefix + vol_id + "/" + g_replayer;
    }
    else{
        key += g_marker_prefix + vol_id + "/" + g_replicator;
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
static RESULT create_journal_file(const string& name) {
    string path = name.substr(0,name.find_last_of('/'));
    //make dir with previlege:read, write, execute/search by group,owner and other
    if(DRS_OK != mkdir_if_not_exist(path.c_str(),S_IRWXG|S_IRWXU|S_IRWXO))
        return INTERNAL_ERROR;
    FILE* file = fopen(name.c_str(), "ab+");
    if(nullptr != file){
        fclose(file);
        return DRS_OK;
    }
    LOG_ERROR << "create journal file " << name << " failed:" << strerror(errno);
    return INTERNAL_ERROR;
}
RESULT delete_journal_file(const string& name) {
    int res = remove(name.c_str());
    if(0 == res || ENOENT == errno) // file deleted or not exist
        return DRS_OK;
    LOG_ERROR << "delete " << name << " failed:" << strerror(res);
    return INTERNAL_ERROR;
}
static string get_journal_filename(const string& vol_id,const int64_t& counter){
    return g_key_prefix + vol_id + "/" + counter_to_string(counter);
}
string construct_sealed_index(const string& m_key){
    string sub = m_key.substr(g_key_prefix.length(),m_key.length()-g_key_prefix.length());
    return g_sealed + sub; 
}
string construct_recyled_index(const string& m_key){
    string sub = m_key.substr(g_key_prefix.length(),m_key.length()-g_key_prefix.length());
    return g_recycled + sub;    
}
string construct_write_open_index(const string& m_key, 
        const string& vol_id, const string& uuid){
    return g_writer_prefix + vol_id + "/" + uuid + "/"
        + m_key.substr(m_key.find_last_of('/')+1);
}

RESULT extract_counter_from_object_key(const string& key,int64_t& cnt){
    std::size_t pos = key.find_last_of('/',string::npos);
    // assert(pos != string::npos);
    string counter_s = key.substr(pos+1,string::npos);
    try {
        cnt = std::stoll(counter_s,nullptr,10);
    }catch (const std::invalid_argument& ia) {
        LOG_ERROR << "Invalid argument: " << ia.what();
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}
RESULT init_cephs3_api(std::unique_ptr<CephS3Api>& s3Api,string& mount_path){
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
    s3Api.reset(new CephS3Api(access_key.c_str(),
            secret_key.c_str(),host.c_str(),bucket_name.c_str()));
    string type;
    if(false == parser->get<string>("journal_storage.type",type)){
        LOG_FATAL << "config parse journal_storage.type error!";
        return INTERNAL_ERROR;
    }
    if(type.compare("ceph_fs") == 0){        
        if(false == parser->get<string>("ceph_fs.mount_point",mount_path)){
            LOG_FATAL << "config parse ceph_fs.mount_point error!";
            return INTERNAL_ERROR;
        }
    }
    else
        DR_ERROR_OCCURED();
    return DRS_OK;
}

RESULT CephS3Meta::get_journal_meta_by_key(const string& key, JournalMeta& meta) {
    auto it = kv_map_.find(key);
    if(it != kv_map_.end()){
        meta.CopyFrom(it->second);
        return DRS_OK;
    }
    string value;
    RESULT res = s3Api_ptr_->get_object(key.c_str(),&value);
    if(res != DRS_OK)
        return res;
    if(true != meta.ParseFromString(value)){
        LOG_ERROR << "parser journal " << key <<" 's meta failed!";
        return INTERNAL_ERROR;
    }
    return DRS_OK; // set meta key same as journal path
}

RESULT CephS3Meta::init_journal_key_counter(const string& vol_id,int64_t& cnt){
    std::list<string> list;
    int64_t counter1=0;
    string prefix = g_key_prefix + vol_id;
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,nullptr,0,&list);
    if(DRS_OK != res) {
        LOG_ERROR << "list volume " << vol_id << " opened journals failed!";
        return res;
    }
    if(!list.empty()){
        if(DRS_OK != extract_counter_from_object_key(list.back(),counter1)){
            return INTERNAL_ERROR;
        }
        cnt = counter1;
        counter_map_.insert(std::pair<string,int64_t>(vol_id,cnt));
        cnt++; // point to next counter
        return DRS_OK;
    }
    else{
        LOG_INFO << "init volume " << vol_id << " journal name counter.";
        cnt = MIN_JOURNAL_COUNTER;
        counter_map_.insert(std::pair<string,int64_t>(vol_id,cnt-1));
        return DRS_OK;
    }    
}
RESULT CephS3Meta::get_journal_key_counter(const string& vol_id,int64_t& cnt){
    auto it = counter_map_.find(vol_id);
    if(counter_map_.end() == it) { // volume not found
        LOG_INFO << "get journal name counter:volume " << vol_id << " not found.";
        return init_journal_key_counter(vol_id,cnt);
    }
    cnt = it->second + 1; // point to next counter
    return DRS_OK;
}
RESULT CephS3Meta::add_journal_key_counter(const string& vol_id,
        const int64_t& add){
    auto it = counter_map_.find(vol_id);
    if(counter_map_.end() == it) { // volume not found
        LOG_ERROR << "set journal name counter failed:volume " << vol_id << " not found.";
        return INTERNAL_ERROR;
    }
    LOG_DEBUG << "update " << vol_id << " journal name counter " 
        << it->second << " ,add " << add;
    it->second = (it->second + add)%MAX_JOURNAL_COUNTER;
    return DRS_OK;
}

// cephS3Meta member functions
CephS3Meta::CephS3Meta() {
    init();
}
CephS3Meta::~CephS3Meta() {
}
RESULT CephS3Meta::init() {   
    RESULT res = init_cephs3_api(s3Api_ptr_,mount_path_);
    if(DRS_OK != res)
        return res;
    return mkdir_if_not_exist((mount_path_+g_key_prefix).c_str(),S_IRWXG|S_IRWXU|S_IRWXO);
}

RESULT CephS3Meta::create_journals(const string& uuid, const string& vol_id,
        const int& limit, std::list<string>& list){
    RESULT res;
    int64_t counter;
    string journals[limit];
    res = get_journal_key_counter(vol_id,counter);
    if(res != DRS_OK)
        return res;
    for(int i=0;i<limit;i++) {
        // TODO:recycle counter?
        int64_t next = (i+counter%MAX_JOURNAL_COUNTER);
        journals[i] = g_key_prefix + vol_id + "/" + counter_to_string(next);
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
    }
    if(DRS_OK != res){
        if(list.size() <= 0)
            return INTERNAL_ERROR;
        // roll back: delete partial meta
        s3Api_ptr_->delete_object(journals[list.size()].c_str());
        string o_key = construct_write_open_index(journals[list.size()],vol_id,uuid);
        s3Api_ptr_->delete_object(o_key.c_str());
        add_journal_key_counter(vol_id,list.size());
        return DRS_OK; // partial success
    }
    add_journal_key_counter(vol_id,limit);
    return res;
}

RESULT CephS3Meta::seal_volume_journals(const string& uuid, const string& vol_id,
        const string journals[], const int& count) {
    RESULT res = DRS_OK;    
    for(int i=0;i<count;i++){
        JournalMeta meta;
        res = get_journal_meta_by_key(journals[i],meta);
        if(DRS_OK != res){
            LOG_ERROR << "get journal " << journals[i] << " meta failed!";
            break;
        }
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
    return res;
}

RESULT CephS3Meta::get_journal_marker(const string& uuid,const string& vol_id,
        const CONSUMER_TYPE& type, JournalMarker* marker){
    string key = assemble_journal_marker_key(vol_id,type);
    unique_ptr<string> p(new string());
    string *value = p.get();
    RESULT res = s3Api_ptr_->get_object(key.c_str(),value);
    // if the consumer failed without init the marker yet? maybe the dr server
    // should init the marker if it's not init, or the restarted consumer may not know where to start    
    if(DRS_OK != res){
        if(NO_SUCH_KEY == res){
            LOG_WARN << vol_id << " 's marker is not initialized!";
            std::list<string> list;
            res = s3Api_ptr_->list_objects((g_key_prefix+vol_id).c_str(),nullptr,nullptr,1,&list);
            if(DRS_OK != res || list.size() <= 0){
                LOG_ERROR << "list volume " << vol_id << " journals failed!";
                return INTERNAL_ERROR;
            }
            marker->set_cur_journal(list.front());
            marker->set_pos(0L);
        }
        else{
            LOG_ERROR << "get journal marker of volume " << vol_id << " failed!";
            return res;
        }
    }
    else
        marker->ParseFromString(*value);
    LOG_INFO << "get marker:" << key << "\n " << marker->cur_journal()
            << ":" << marker->pos();
    return DRS_OK;
}
RESULT CephS3Meta::update_journals_marker(const string&uuid, const string& vol_id,
        const CONSUMER_TYPE& type, const JournalMarker& marker){
    string key = assemble_journal_marker_key(vol_id,type);
    string marker_s;
    if(false==marker.SerializeToString(&marker_s)){
        LOG_ERROR << vol_id << " serialize marker failed!";
        return INTERNAL_ERROR;
    }
    RESULT res = s3Api_ptr_->put_object(key.c_str(),&marker_s,nullptr);
    if(DRS_OK != res){
        LOG_ERROR << "update_journals_marker of volume " << vol_id << " failed!";
        return res;
    }
    // TODO: delete journals files which were consumed by both replyer and replicator
    return DRS_OK;
}

RESULT CephS3Meta::get_consumable_journals(const string& uuid,const string& vol_id,
        const JournalMarker& marker, const int& limit, std::list<string> &list) {
    if(!marker.IsInitialized()) {
        LOG_ERROR << vol_id << " 's marker is not initialized!";
        return INTERNAL_ERROR;
    }
    const char* marker_key = marker.cur_journal().c_str();
    string prefix = g_key_prefix+vol_id;
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),marker_key,nullptr,limit,&list);
    if(DRS_OK != res){
        LOG_ERROR << "list volume " << vol_id << " journals failed!";
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

// gc manager
RESULT CephS3Meta::get_sealed_and_consumed_journals(const string& vol_id,
        const int& limit, std::list<string>& list){
    JournalMarker marker;
    string end_marker;
    string key = assemble_journal_marker_key(vol_id,REPLAYER);
    unique_ptr<string> p(new string());
    RESULT res = s3Api_ptr_->get_object(key.c_str(),p.get());
    if(DRS_OK != res){
        if(NO_SUCH_KEY == res)
            return DRS_OK; // no journal
        else
            return INTERNAL_ERROR;
    }    
    if(false==marker.ParseFromString(*(p.get()))){
        LOG_ERROR << "parser from string failed!";
        return INTERNAL_ERROR;
    }
    end_marker = marker.cur_journal();
    p->clear();
    marker.Clear();
    key = assemble_journal_marker_key(vol_id,REPLICATER);
    res = s3Api_ptr_->get_object(key.c_str(),p.get());
    if(DRS_OK != res){
        if(NO_SUCH_KEY == res)
            return DRS_OK; // no journal
        else
            return INTERNAL_ERROR;
    } 
    if(false==marker.ParseFromString(*(p.get()))){
        LOG_ERROR << "parser from string failed!";
        return INTERNAL_ERROR;
    }
    end_marker = end_marker.compare(marker.cur_journal()) < 0 ? 
        end_marker : marker.cur_journal();  // set the lower journal key as end_marker
    string prefix = g_key_prefix+vol_id;
    return s3Api_ptr_->list_objects(prefix.c_str(),nullptr,end_marker.c_str(),
        limit,&list);    
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
    }
    return res;
}
// TODO: recycled_journals delete or reuse
RESULT CephS3Meta::get_recycled_journals(const string& vol_id,
        const int& limit, std::list<string>& list){
    return INTERNAL_ERROR;
}
RESULT CephS3Meta::delete_journals(const string& vol_id,
        const std::list<string>& journals){
    return INTERNAL_ERROR;
}
RESULT CephS3Meta::get_producer_id(const string& vol_id,
        std::list<string>& list){
    string prefix = g_writer_prefix+vol_id+"/";
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,nullptr,0,&list,"/");
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
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,nullptr,0,&list);
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
    string prefix = g_writer_prefix;
    RESULT res = s3Api_ptr_->list_objects(prefix.c_str(),nullptr,nullptr,10,&list,"/");
    if(DRS_OK != res){
        LOG_ERROR << "list objects failed:" << prefix;
        return res;
    }
    LOG_DEBUG << "list volumes :" << list.size();
    if(list.empty())
        return DRS_OK;
    std::for_each(list.begin(),list.end(),[](string& s){
        // get "/vol-id/" in "/sessions/writer/vol-id/"
        size_t end = s.find_last_of('/');
        size_t pos = s.find_last_of('/',end-1);
        s = s.substr(pos+1,end-pos-1);// remove '/' at start and end
    });
    return DRS_OK;
    return DRS_OK;
}
