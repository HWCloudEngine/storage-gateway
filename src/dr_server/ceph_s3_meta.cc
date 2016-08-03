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
#define MAX_UPDATES 10
const string g_journal_id_key = "/journals/counters";
const string g_key_prefix = "/journals/";
const string g_marker_prefix = "/markers/";
const string g_replayer = "replayer";
const string g_replicator = "replicator";
using std::unique_ptr;

int S3_status_is_retryable(S3Status status)
{
    switch (status) {
        case S3StatusNameLookupError:
        case S3StatusFailedToConnect:
        case S3StatusConnectionFailed:
        case S3StatusErrorInternalError:
        case S3StatusErrorOperationAborted:
        case S3StatusErrorRequestTimeout:
        return 1;
    default:
        return 0;
    }
}

static int should_retry(s3_call_response_t &response)
{
    if (response.retries--) {
        // Sleep before next retry; start out with a 1 second sleep
        sleep(response.retrySleepInterval);
        // Next sleep 1 second longer
        response.retrySleepInterval++;
        return 1;
    }
    return 0;
}

S3Status responsePropertiesCallback(
        const S3ResponseProperties *properties, void *callbackData){
    return S3StatusOK;
}
static void responseCompleteCallback(S3Status status,
        const S3ErrorDetails *error, void *callbackData){
    s3_call_response_t* response = (s3_call_response_t*) callbackData;
    response->status = status;
    if(status != S3StatusOK){
        std::cerr << error->message << std::endl;
    }
    return;
}

S3ResponseHandler responseHandler = {
   &responsePropertiesCallback,
   &responseCompleteCallback
};

static int putObjectDataCallback(int bufferSize, char *buffer,
        void *callbackData){    
    s3_call_response_t* response = (s3_call_response_t*)callbackData;
    if(NULL == response->pdata){
        return 0;
    }
    string* meta_s = (string*)(response->pdata);
    int res = 0;
    if(response->size > 0) {
        res = (response->size > bufferSize) ? bufferSize:response->size;
        strncpy(buffer,meta_s->c_str()+(meta_s->length()-response->size),res);
        response->size -= res;
    }
//    std::cout << "obj len:" << meta_s->length() << " left:" << response->size << std::endl;    
    return res; 
}

static S3Status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData)
{
    string *value = (string*)(((s3_call_response_t*)callbackData)->pdata);
    value->append(buffer,bufferSize);
    LOG_DEBUG << "getObjectDataCallback, len:" << value->length() << ":" << bufferSize;
    return S3StatusOK;
}

static S3Status listBucketCallback(int isTruncated, const char *nextMarker,
        int contentsCount, const S3ListBucketContent *contents, int commonPrefixesCount,
        const char **commonPrefixes, void *callbackData){
    s3_call_response_t* response = (s3_call_response_t*)callbackData;
    response->isTruncated = isTruncated;
    // This is tricky.  S3 doesn't return the NextMarker if there is no
    // delimiter. since it's still useful for paging through results. 
    // We want NextMarker to be the last content in the list,
    // so set it to that if necessary.
    if ((!nextMarker || !nextMarker[0]) && contentsCount) {
        nextMarker = contents[contentsCount - 1].key;
    }
    if (nextMarker) {
        snprintf(response->nextMarker, sizeof(response->nextMarker), "%s", nextMarker);
    }
    else {
        response->nextMarker[0] = 0;
    }
    std::list<string>* list = (std::list<string>*)(response->pdata);
    LOG_DEBUG << "list objects:";
    for (int i = 0; i < contentsCount; i++) {
        const S3ListBucketContent *content = &(contents[i]);
        string key(content->key);
        list->push_back(key);
        LOG_DEBUG << content->key;
    }
    response->keyCount += contentsCount;
    return S3StatusOK;
}

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

// cephS3Meta member functions
CephS3Meta::CephS3Meta() {    
}
CephS3Meta::~CephS3Meta() {
    if(counters_updates) {
        RESULT res = update_volume_journalsID_counter();
        if(res!=DRS_OK) {
            LOG_ERROR << "update journalsID counter failed in destructor!";
        }
    }
    S3_deinitialize();
}
RESULT CephS3Meta::init(const char* access_key, const char* secret_key, const char* host,
        const char* bucket_name, const char* path) {
    S3Status status;
    bucketContext.accessKeyId = access_key;
    bucketContext.secretAccessKey = secret_key;
    bucketContext.hostName = host;
    bucketContext.bucketName = bucket_name;
    bucketContext.protocol = S3ProtocolHTTP;
    bucketContext.uriStyle = S3UriStylePath;
    counters_updates = 0;
    _mount_path = path;
    status = S3_initialize("s3", S3_INIT_ALL, host);
    if(status != S3StatusOK) {
        LOG_FATAL << "Failed to initialize libs3:: " << S3_get_status_name(status);
        return INTERNAL_ERROR;
    }
    RESULT res = create_bucket_if_not_exists(bucket_name);
    if(res != DRS_OK){
        LOG_ERROR << "create bucket failed!";
        return res;
    }
    res = restore_volume_journalsID_counter();
    return res;
}

RESULT CephS3Meta::create_bucket_if_not_exists(const char* bucket_name) {
    s3_call_response_t response;
    response.status = S3StatusHttpErrorUnknown;
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do{
        S3_test_bucket(bucketContext.protocol,bucketContext.uriStyle,
                bucketContext.accessKeyId,bucketContext.secretAccessKey,
                bucketContext.hostName, bucket_name,0,NULL,NULL,
                &responseHandler,&response);
    }while(S3_status_is_retryable(response.status)
                && should_retry(response));
    if(S3StatusOK == response.status) {
        return DRS_OK; // bucket exists
    }
    else if(S3StatusErrorNoSuchBucket != response.status) {
        LOG_ERROR << "test bucket:" << bucket_name << " failed!";
        return INTERNAL_ERROR;
    }
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do{
        S3_create_bucket(bucketContext.protocol, bucketContext.accessKeyId,
                bucketContext.secretAccessKey,bucketContext.hostName, bucket_name,
                S3CannedAclPrivate, NULL, NULL, &responseHandler, NULL);
    }while(S3_status_is_retryable(response.status)
                && should_retry(response));
    if(S3StatusOK == response.status) {
        return DRS_OK;
    }
    LOG_ERROR << "create bucket:" << bucket_name << " failed!";
    return INTERNAL_ERROR;
}

void CephS3Meta::put_object(const char* obj_name, s3_call_response_t& response) {
    S3PutObjectHandler putObjectHandler =
    {
        responseHandler,
        &putObjectDataCallback
    };
    string *value = (string*) (response.pdata);
    int64_t len = 0;
    if(nullptr != value) {
        len = value->length();
    }
    LOG_DEBUG << "put object " << obj_name << ",length: " << len;
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do {
        S3_put_object(&bucketContext,obj_name,len,nullptr,
                nullptr,&putObjectHandler,&response);
    } while(S3_status_is_retryable(response.status)
                && should_retry(response));
    return;
}

void CephS3Meta::get_object(const char* key, s3_call_response_t& response){
    S3GetObjectHandler getObjectHandler =
    {
        responseHandler,
        &getObjectDataCallback
    };
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do {
        S3_get_object(&bucketContext, key, NULL, 0, 0, NULL,
                &getObjectHandler, &response);
    } while(S3_status_is_retryable(response.status)
                && should_retry(response));
    return;
}

void CephS3Meta::delete_object(const char* key,s3_call_response_t& response) {
    S3ResponseHandler deleteResponseHandler = {
        0,
        &responseCompleteCallback
    };
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do {
        S3_delete_object(&bucketContext, key, NULL, &deleteResponseHandler, &response);
    } while(S3_status_is_retryable(response.status)
                && should_retry(response));
    return;
}

void CephS3Meta::list_objects(const char*prefix, const char*marker, int maxkeys,
            s3_call_response_t& response) {
    S3ListBucketHandler listBucketHandler =
    {
        responseHandler,
        &listBucketCallback
    };
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    response.keyCount = 0;
    if(NULL!=marker && 0!=marker[0]){
        snprintf(response.nextMarker, sizeof(response.nextMarker), "%s", marker);
    }
    else {
        response.nextMarker[0] = 0;
    }
    do {
        response.isTruncated = 0;
        do {
            S3_list_bucket(&bucketContext,prefix,response.nextMarker,
                NULL/*delimiter*/,maxkeys,NULL,&listBucketHandler,&response);
        } while(S3_status_is_retryable(response.status)
                && should_retry(response));
        if(response.status != S3StatusOK)
            break;
    } while(response.isTruncated && (!maxkeys || (response.keyCount < maxkeys)));
    return;
}

RESULT CephS3Meta::update_journals_meta(string vol_id, string journals[],
        int count, JOURNAL_STATUS status){   
    for(int i=0; i < count; i++) {
        s3_call_response_t response;
        JournalMeta meta;
        string key = get_journal_key(vol_id,journals[i]);
        meta.set_path(journals[i]);
        meta.set_status(status);
        string meta_s;
        meta.SerializeToString(&meta_s);
        response.size = meta_s.length();
        response.pdata = &meta_s;
        put_object(key.c_str(),response);
        // TODO: update open/sealed journals list
        if(OPENED == status){
        }
        else if(SEALED == status){
        }
        if(S3StatusOK != response.status){
            LOG_ERROR << "update object failed:" << response.status;
            return INTERNAL_ERROR;
        }
    }
    return DRS_OK;
}

RESULT CephS3Meta::restore_volume_journalsID_counter(){
    unique_ptr<string> p(new string());
    s3_call_response response;
    string* value = p.get();
    response.pdata = value;
    response.status = S3StatusHttpErrorUnknown;

    get_object(g_journal_id_key.c_str(),response);
    if(S3StatusHttpErrorNotFound == response.status) {
        LOG_INFO << "restore_volume_journalsID_counter not found.";
        counters.ParseFromString(*value);
        return DRS_OK;
    }
    else if(S3StatusOK != response.status) {
        LOG_ERROR << "restore_volume_journalsID_counter error:" << response.status;
        return INTERNAL_ERROR;
    }
    if(true != counters.ParseFromString(*value)) { //deserialize journalsID counters
        LOG_ERROR << "restore_volume_journalsID_counter: parseFromString failed!";
    }    
    // force every volume journalIDs add by MAX_UPDATES at initialization,
    // this could avoid to use repeated counters if drserver restart.
    auto map = counters.mutable_ids();
    LOG_DEBUG << "volume ids:" << counters.ids().size();
    for(auto it=map->begin(); it!=map->end(); ++it){
        it->second += MAX_UPDATES;
        LOG_DEBUG << "  " << it->first << ":" << it->second;
    }    
    return DRS_OK;
}

// save journasId counter if MAX_UPDATES updates happened or new volume is added
RESULT CephS3Meta::update_volume_journalsID_counter(){
    unique_ptr<string> p(new string());
    string *value = p.get();
    s3_call_response response;
    response.pdata = value;    
    if(false == counters.SerializeToString(value)){
        LOG_ERROR << "serialize journalsID counters failed!";
        return INTERNAL_ERROR;
    }
    response.status = S3StatusHttpErrorUnknown;
    response.size = value->length();
    put_object(g_journal_id_key.c_str(),response);
    if(response.status != S3StatusOK) {
        LOG_ERROR << "updating journalsID counters failed:" << response.status;
        return INTERNAL_ERROR;
    }
    //debug
    auto map = counters.ids();
    LOG_DEBUG << "update journalsID counter,size:" << map.size();
    for(auto it=map.begin(); it!=map.end(); ++it) {
        LOG_DEBUG << "  " << it->first << ":" << it->second;
    }
    return DRS_OK;
    
}

RESULT CephS3Meta::get_volume_journalsID_counter(string vol_id, int64& cnt) {
    RESULT res = DRS_OK;
    auto* map = counters.mutable_ids();
    auto it = map->find(vol_id);
    if(map->end() == it) {
        (*map)[vol_id] = 0; // volume id counter not found, init it as 0;
        cnt = 0;
        res = update_volume_journalsID_counter(); // update journasId if new volume added
        LOG_INFO << "init volume:" << vol_id << "'s journalsId counter.";
    }
    else {
        cnt = it->second;
        LOG_INFO << "get volume:" << it->first << "'s journalsId counter:" << cnt;
    }
    return res;
}

RESULT CephS3Meta::get_volume_journals(string vol_id, int limit,
        std::list<string>& list){
    RESULT result;
    int64 counter;
    string journals[limit];
    result = get_volume_journalsID_counter(vol_id,counter);
    if(result != DRS_OK)
        return result;
    for(int i=0;i<limit;i++) {
        journals[i] = assemble_journal_key(vol_id, i+counter);
    }
    auto* map = counters.mutable_ids();
    (*map)[vol_id] += limit;
    counters_updates += limit;
    if(counters_updates >= MAX_UPDATES){    
        result = update_volume_journalsID_counter();
        if(result == DRS_OK) {
            counters_updates = 0;
        }
        else{
            LOG_ERROR << "update volume journalsID counters failed!";
        }
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
    return DRS_OK;
}

// TODO: it seems like that a thread is needed to check and delete unwanted journal files and their metas
RESULT CephS3Meta::delete_journals(string vol_id, string journals[], int count){
    for(int i=0; i<count; i++) {        
        string key = get_journal_key(vol_id,journals[i]);
        s3_call_response_t response;
        response.status = S3StatusOK;
        delete_object(key.c_str(),response);
        if(S3StatusOK != response.status) {
            LOG_ERROR << "delete journals meta failed!";
            return INTERNAL_ERROR;
        }
        // TODO: delete journal files
    }
    return DRS_OK;
}
RESULT CephS3Meta::seal_volume_journals(string vol_id, string journals[], int count) {
    return update_journals_meta(vol_id,journals,count,SEALED);
}

RESULT CephS3Meta::get_journal_marker(string vol_id, CONSUMER_TYPE type,
        JournalMarker* marker){
    string key = assemble_journal_marker_key(vol_id,type);
    s3_call_response_t response;
    unique_ptr<string> p(new string());
    string *value = p.get();
    response.pdata = (void*) value;
    get_object(key.c_str(),response);
    if(S3StatusOK != response.status){
        LOG_ERROR << "get journal marker failed!";
        return INTERNAL_ERROR;
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
    s3_call_response_t response;
    response.pdata = (void*)&marker_s;
    put_object(key.c_str(),response);
    if(S3StatusOK != response.status){
        LOG_ERROR << "update_journals_marker failed!";
        return INTERNAL_ERROR;
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
    s3_call_response_t response;
    response.pdata = (void*)(&list);
    list_objects(prefix.c_str(),marker_key,limit,response);
    if(S3StatusOK != response.status){
        LOG_ERROR << "list volume " << vol_id << " sealed journals failed!";
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

