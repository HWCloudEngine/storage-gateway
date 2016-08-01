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
#define MAX_JOURNAL_COUNTER 1000
#define MIN_JOURNAL_COUNTER 1
const string g_key_prefix = "/journals/";
const string g_marker_prefix = "/markers/";
const string g_opened = "/opened/";
const string g_sealed = "/sealed/";
const string g_replayer = "replayer";
const string g_replicator = "replicator";
using std::unique_ptr;

static string assemble_journal_key(string vol_id, int64_t counter) {
    char tmp[9] = {0};
    std::sprintf(tmp,"%08ld",counter);
    string counter_s(tmp);
    string key = g_key_prefix + vol_id + "/" + counter_s;
    return key;
}
static string get_journal_key(string vol_id, string path) {
    return path; // set meta key same as journal path
}
static string assemble_journal_marker_key(string vol_id, CONSUMER_TYPE type) {    
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
static RESULT create_journal_file(string name) {
    FILE* file = fopen(name.c_str(), "ab+");
    if(nullptr != file){
        fclose(file);
        return DRS_OK;
    }
    LOG_ERROR << "create journal file " << name << " failed:" << strerror(errno);
    return INTERNAL_ERROR;
}
static RESULT delete_journal_file(string name) {
    int res = remove(name.c_str());
    if(0 == res)
        return DRS_OK;
    LOG_ERROR << "delete " << name << " failed:" << strerror(res);
    return INTERNAL_ERROR;
}

void GC_task(){
    while(CephS3Meta::_thread_GC_running){
		// TODO: journal GC
        sleep(1);
    }
}

RESULT CephS3Meta::init_journal_name_counter(const string vol_id,int64_t& cnt){
    std::list<string> list;
    string prefix = g_opened + vol_id;
    RESULT res = _s3Api_ptr->list_objects(prefix.c_str(),nullptr,0,&list);
    if(DRS_OK != res) {
        LOG_ERROR << "list volume " << vol_id << " opened journals failed!";
        return res;
    }
    if(list.empty()) {
        prefix = g_sealed + vol_id;
        res = _s3Api_ptr->list_objects(prefix.c_str(),nullptr,0,&list);
        if(DRS_OK != res) {
            LOG_ERROR << "list volume " << vol_id << " sealed journals failed!";
            return res;
        }
        if(list.empty()){
            LOG_INFO << "init volume " << vol_id << " journal name counter.";
            cnt = MIN_JOURNAL_COUNTER;
            return DRS_OK;
        }
    }
    // TODO:recycle the counter
    auto it = list.back();
    std::size_t pos = it.find_last_of('/',string::npos);
    // assert(pos != string::npos);
    string counter_s = it.substr(pos+1,string::npos);
    try {
        int64_t counter = std::stoll(counter_s,nullptr,10);
        _map.insert(std::pair<string,int64_t>(vol_id,counter));
        cnt = counter+1; // point to next counter
    }catch (const std::invalid_argument& ia) {
        LOG_ERROR << "Invalid argument: " << ia.what();
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}
RESULT CephS3Meta::get_journal_name_counter(const string vol_id,int64_t& cnt){
    auto it = _map.find(vol_id);
    if(_map.end() == it) { // volume not found
        LOG_INFO << "get journal name counter:volume " << vol_id << " not found.";
        return init_journal_name_counter(vol_id,cnt);
    }
    cnt = it->second + 1; // point to next counter
    return DRS_OK;
}
RESULT CephS3Meta::add_journal_name_counter(const string vol_id,const int64_t add){
    auto it = _map.find(vol_id);
    if(_map.end() == it) { // volume not found
        LOG_ERROR << "set journal name counter failed:volume " << vol_id << " not found.";
        return INTERNAL_ERROR;
    }
    LOG_DEBUG << "update " << vol_id << " journal name counter " 
        << it->second << " add to " << add;
    it->second += add;
    return DRS_OK;
}

// cephS3Meta member functions
CephS3Meta::CephS3Meta() {    
}
CephS3Meta::~CephS3Meta() {
    CephS3Meta::_thread_GC_running = false;
    if(_thread_GC.joinable())
        _thread_GC.join();
}
RESULT CephS3Meta::init(const char* access_key, const char* secret_key, const char* host,
        const char* bucket_name, const char* path) {
    S3Status status;
    _s3Api_ptr = unique_ptr<CephS3Api>(new CephS3Api(access_key,secret_key,host,bucket_name));
    _mount_path = path;
    return DRS_OK;
}

RESULT CephS3Meta::update_journals_meta(string vol_id, string journals[],
        int count, JOURNAL_STATUS status){
    RESULT res = DRS_OK;
    for(int i=0; i < count; i++) {
        JournalMeta meta;
        string key = get_journal_key(vol_id,journals[i]);
        meta.set_path(journals[i]);
        meta.set_status(status);
        string meta_s;
        meta.SerializeToString(&meta_s);        
        //res = _s3Api_ptr->put_object(key.c_str(),&meta_s);
        if(OPENED == status){
            string o_key = g_opened + vol_id + key;
            res = _s3Api_ptr->put_object(o_key.c_str(),nullptr);
        }
        else if(SEALED == status){
            string s_key = g_sealed + vol_id + key;
            res = _s3Api_ptr->put_object(s_key.c_str(),nullptr);
        }
        if(DRS_OK != res) {
            LOG_ERROR << "put journal key meta failed:" << key;
            break;
        }
    }
    return res;
}

RESULT CephS3Meta::get_volume_journals(string vol_id, int limit,
        std::list<string>& list){
    RESULT result;
    int64_t counter;
    string journals[limit];
    result = get_journal_name_counter(vol_id,counter);
    if(result != DRS_OK)
        return result;
    for(int i=0;i<limit;i++) {
        // TODO:recycle counter
        int64_t next = (i+counter%MAX_JOURNAL_COUNTER);
        journals[i] = assemble_journal_key(vol_id, next);
    }   
    result = update_journals_meta(vol_id,journals,limit,OPENED);
    if(DRS_OK != result){
        return INTERNAL_ERROR;
    }
    std::list<string> temp(journals,journals+sizeof(journals)/sizeof(string));
    list.swap(temp);
    //create ceph volume path and journal files
    string path = _mount_path + g_key_prefix + vol_id;
    if(DRS_OK != mkdir_if_not_exist(path.c_str(),S_IRWXG|S_IRWXU|S_IRWXO)){ //read, write, execute/search by group,owner and others
        LOG_ERROR << "crate volume " << vol_id << " journals dir failed.";
        // TODO: delete journal meta
        return INTERNAL_ERROR;
    }
    for(auto it=list.begin(); it!=list.end(); it++) {        
        create_journal_file(_mount_path+(*it));
    }
    result = add_journal_name_counter(vol_id,limit);
    return result;
}

RESULT CephS3Meta::delete_journals(string vol_id, string journals[], int count){
    for(int i=0; i<count; i++) {        
        string key = get_journal_key(vol_id,journals[i]);     
        // TODO: delete journal files
        RESULT res = _s3Api_ptr->delete_object(key.c_str());
        if(DRS_OK != res){
            LOG_ERROR << "delete journals of volume " << vol_id << " failed!";
            return res;
        }
    }
    return DRS_OK;
}
RESULT CephS3Meta::seal_volume_journals(string vol_id, string journals[], int count) {
    return update_journals_meta(vol_id,journals,count,SEALED);
}

RESULT CephS3Meta::get_journal_marker(string vol_id, CONSUMER_TYPE type,
        JournalMarker* marker){
    string key = assemble_journal_marker_key(vol_id,type);
    unique_ptr<string> p(new string());
    string *value = p.get();
    RESULT res = _s3Api_ptr->get_object(key.c_str(),value);
    if(DRS_OK != res){
        LOG_ERROR << "get journal marker of volume " << vol_id << " failed!";
        return res;
    }    
    marker->ParseFromString(*value);
    LOG_INFO << "get marker:" << key << "\n " << marker->cur_journal()
            << ":" << marker->pos();
    return DRS_OK;
}
RESULT CephS3Meta::update_journals_marker(string vol_id, CONSUMER_TYPE type,
        JournalMarker marker){
    string key = assemble_journal_marker_key(vol_id,type);
    string marker_s;
    marker.SerializeToString(&marker_s);
    RESULT res = _s3Api_ptr->put_object(key.c_str(),&marker_s);
    if(DRS_OK != res){
        LOG_ERROR << "update_journals_marker of volume " << vol_id << " failed!";
        return res;
    }
    // TODO: delete journals files which were consumed by both replyer and replicator
    return DRS_OK;
}

RESULT CephS3Meta::get_consumer_journals(string vol_id,JournalMarker marker,
        int limit, std::list<string> &list) {
    const char *marker_key = NULL;
    if(marker.IsInitialized()) {
        string start_key = get_journal_key(vol_id,marker.cur_journal());
        marker_key = start_key.c_str();
    }
    string prefix = g_key_prefix+vol_id;
    RESULT res = _s3Api_ptr->list_objects(prefix.c_str(),marker_key,limit,&list);
    if(DRS_OK != res){
        LOG_ERROR << "list volume " << vol_id << " sealed journals failed!";
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

// TODO:GC thread to check and delete unwanted journal files and their metas, and seal journals of crushed client
bool CephS3Meta::_thread_GC_running = false;
void CephS3Meta::init_GC_thread(){
    if(CephS3Meta::_thread_GC_running) {
        LOG_WARN << "journals GC thread was already init.";
        return;
    }
    CephS3Meta::_thread_GC_running = true;
    _thread_GC = ::std::thread(GC_task);    
    LOG_INFO << "init GC thread, id=" << _thread_GC.get_id();
    return;
}


