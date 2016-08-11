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
    return DRS_OK;
}

RESULT CephS3Api::put_object(const char* obj_name, const string* value) {
    return DRS_OK;
}

RESULT CephS3Api::get_object(const char* key, string* value){
    return DRS_OK;
}

RESULT CephS3Api::delete_object(const char* key) {
    return DRS_OK;
}

RESULT CephS3Api::list_objects(const char*prefix, const char*marker, int maxkeys,
            std::list<string>* list) {
    return DRS_OK;
}