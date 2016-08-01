/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    ceph_s3_api.cc
* Author: 
* Date:         2016/08/08
* Version:      1.0
* Description:
* 
************************************************/
#include <stdlib.h>
#include "ceph_s3_api.h"
#include "log/log.h"
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif
#ifndef MAX_RETRIES
#define MAX_RETRIES 3
#endif
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

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

CephS3Api::CephS3Api(const char* access_key, const char* secret_key, const char* host,
            const char* bucket_name) {
    bucketContext.accessKeyId = access_key;
    bucketContext.secretAccessKey = secret_key;
    bucketContext.hostName = host;
    bucketContext.bucketName = bucket_name;
    bucketContext.protocol = S3ProtocolHTTP;
    bucketContext.uriStyle = S3UriStylePath;
    S3Status status = S3_initialize("s3", S3_INIT_ALL, host);
    if(status != S3StatusOK) {
        LOG_FATAL << "Failed to initialize libs3:: " << S3_get_status_name(status);
        exit(EXIT_FAILURE);
    }
}
CephS3Api::~CephS3Api() {
    S3_deinitialize();
}

RESULT CephS3Api::create_bucket_if_not_exists(const char* bucket_name) {
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
        LOG_ERROR << "test bucket:" << bucket_name << " failed:"
            <<  S3_get_status_name(response.status);
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
    if(S3StatusOK != response.status) {
        LOG_ERROR << "create bucket:" << bucket_name << " failed:"
            <<  S3_get_status_name(response.status);
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

RESULT CephS3Api::put_object(const char* obj_name, const string* value) {
    S3PutObjectHandler putObjectHandler =
    {
        responseHandler,
        &putObjectDataCallback
    };
    int64_t len = 0;
    if(nullptr != value)
        len = value->length();
    LOG_DEBUG << "put object " << obj_name << ",length: " << len;
    s3_call_response_t response;
    response.pdata = (void*)(value);
    response.size = len;
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do {
        S3_put_object(&bucketContext,obj_name,len,nullptr,
                nullptr,&putObjectHandler,&response);
    } while(S3_status_is_retryable(response.status)
                && should_retry(response));
    if(S3StatusOK != response.status){
        LOG_ERROR << "update object failed:" << S3_get_status_name(response.status);;
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

RESULT CephS3Api::get_object(const char* key, string* value){
    S3GetObjectHandler getObjectHandler =
    {
        responseHandler,
        &getObjectDataCallback
    };
    s3_call_response_t response;
    response.pdata = value;
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do {
        S3_get_object(&bucketContext, key, NULL, 0, 0, NULL,
                &getObjectHandler, &response);
    } while(S3_status_is_retryable(response.status)
                && should_retry(response));
    if(S3StatusOK != response.status) {
        LOG_ERROR << "get object " << key << " failed:"
            << S3_get_status_name(response.status);
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

RESULT CephS3Api::delete_object(const char* key) {
    S3ResponseHandler deleteResponseHandler = {
        0,
        &responseCompleteCallback
    };
    s3_call_response_t response;
    response.retries = MAX_RETRIES;
    response.retrySleepInterval = SLEEP_UNITS_PER_SECOND;
    do {
        S3_delete_object(&bucketContext, key, NULL, &deleteResponseHandler, &response);
    } while(S3_status_is_retryable(response.status)
                && should_retry(response));
    if(S3StatusErrorNoSuchKey == response.status)
        return DRS_OK; // object key not found
    if(S3StatusOK != response.status) {
        LOG_ERROR << "delete object " << key << " failed:" << S3_get_status_name(response.status);
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}

RESULT CephS3Api::list_objects(const char*prefix, const char*marker, int maxkeys,
            std::list<string>* list) {
    S3ListBucketHandler listBucketHandler =
    {
        responseHandler,
        &listBucketCallback
    };
    s3_call_response_t response;
    response.pdata = (void*)list;
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
    if(S3StatusOK != response.status){
        LOG_ERROR << "list object failed: " << S3_get_status_name(response.status);
        return INTERNAL_ERROR;
    }
    return DRS_OK;
}