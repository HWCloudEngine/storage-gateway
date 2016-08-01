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
#include <iostream>
#include <unistd.h>
#include <memory>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include "ceph_s3_meta.h"
#include "log/log.h"
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif
#ifndef MAX_RETRIES
#define MAX_RETRIES 3
#endif
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
    if(DRS_OK != mkdir_if_not_exist(path.c_str(),S_IRWXG|S_IRWXU|S_IRWXO)) //make dir with previlege:read, write, execute/search by group,owner and other
        return INTERNAL_ERROR;
    FILE* file = fopen(name.c_str(), "ab+");
    if(nullptr != file){
        fclose(file);
        return DRS_OK;
    }
    LOG_ERROR << "create journal file " << name << " failed:" << strerror(errno);
    return INTERNAL_ERROR;
}
static RESULT delete_journal_file(const string& name) {
    int res = remove(name.c_str());
    if(0 == res || ENOENT == errno) // file deleted or not exist
        return DRS_OK;
    LOG_ERROR << "delete " << name << " failed:" << strerror(res);
    return INTERNAL_ERROR;
}
static string get_journal_filename(const string& vol_id,const int64_t& counter){
    return g_key_prefix + vol_id + "/" + counter_to_string(counter);
}
void GC_task(std::shared_ptr<CephS3Api> api_ptr){
    api_ptr->put_object("aaaa",nullptr,nullptr);
    while(CephS3Meta::thread_GC_running_){
    // TODO: journal GC
        sleep(1);
    }
}
static RESULT extract_counter_from_object_key(const string& key,int64_t& cnt){
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
RESULT CephS3Meta::add_journal_key_counter(const string& vol_id,const int64_t& add){
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
}
CephS3Meta::~CephS3Meta() {
    CephS3Meta::thread_GC_running_ = false;
    if(thread_GC_.joinable())
        thread_GC_.join();
}
RESULT CephS3Meta::init(const char* access_key, const char* secret_key, const char* host,
        const char* bucket_name, const char* path) {
    S3Status status;
    s3Api_ptr_ = unique_ptr<CephS3Api>(new CephS3Api(access_key,secret_key,host,bucket_name));
    mount_path_ = path;
    return DRS_OK;
}

RESULT CephS3Meta::get_volume_journals(const string& uuid, const string& vol_id,
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
        journals[i] = g_key_prefix+vol_id+"/" + counter_to_string(next);
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
        string o_key = g_writer_prefix + vol_id + "/" + uuid + "/" + counter_to_string(next);
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
        string o_key = g_writer_prefix + vol_id + "/" + uuid + "/" 
            + counter_to_string(counter+list.size());
        s3Api_ptr_->delete_object(o_key.c_str());
        add_journal_key_counter(vol_id,list.size());
        return DRS_OK; // partial success
    }
    add_journal_key_counter(vol_id,limit);
    return res;
}

RESULT CephS3Meta::mark_journal_recycled(const string& uuid, const string& vol_id, const string& key){
    RESULT res = DRS_OK;
    string value;
    res = s3Api_ptr_->get_object(key.c_str(),&value);
    if(DRS_OK != res){
        LOG_ERROR << "read obj " << key << " failed!";
        return res;
    }
    string sub = key.substr(g_key_prefix.length(),key.length()-g_key_prefix.length());
    string r_key = g_recycled + sub;
    res = s3Api_ptr_->put_object(r_key.c_str(),&value,nullptr);
    if(DRS_OK != res){
        LOG_ERROR << "put obj " << r_key << " failed!";
        return res;
    }
    string s_key = g_sealed + sub;
    res = s3Api_ptr_->delete_object(s_key.c_str());
    if(DRS_OK != res){
        LOG_ERROR << "delete key " << s_key << " failed!";
        return res;
    }
    int64_t counter;
    extract_counter_from_object_key(s_key,counter);
    string o_key = g_writer_prefix + vol_id + "/" + uuid + "/" + counter_to_string(counter);
    res = s3Api_ptr_->delete_object(o_key.c_str());
    if(DRS_OK != res){
        LOG_ERROR << "delete key " << o_key << " failed!";
        return res;
    }
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
        string sub = journals[i].substr(g_key_prefix.length(),journals[i].length()-g_key_prefix.length());
        string s_key = g_sealed + sub;
        res = s3Api_ptr_->put_object(s_key.c_str(),nullptr,nullptr); // add journal sealed index key
        if(DRS_OK != res){
            LOG_ERROR << "add sealed journal's index key " << s_key << " failed!";
            break;
        }
        int64_t counter;
        extract_counter_from_object_key(s_key,counter);
        string o_key = g_writer_prefix + vol_id + "/" + uuid + "/" + counter_to_string(counter);
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

RESULT CephS3Meta::get_consumer_journals(const string& uuid,const string& vol_id,
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

// TODO:GC thread to check and delete unwanted journal files and their metas, and seal journals of crushed client
bool CephS3Meta::thread_GC_running_ = false;
void CephS3Meta::init_GC_thread(){
    if(CephS3Meta::thread_GC_running_) {
        LOG_WARN << "journals GC thread was already init.";
        return;
    }
    CephS3Meta::thread_GC_running_ = true;
    thread_GC_ = ::std::thread(GC_task,std::ref(s3Api_ptr_));
    LOG_INFO << "init GC thread, id=" << thread_GC_.get_id();
    return;
}

