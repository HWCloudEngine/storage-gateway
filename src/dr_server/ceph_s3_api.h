/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    ceph_s3_api.h
* Author: 
* Date:         2016/08/08
* Version:      1.0
* Description:
* 
************************************************/
#ifndef CEPH_S3_API_H_
#define CEPH_S3_API_H_
#include <cstdint>
#include <string>
#include <list>
#include "libs3.h" // require ceph-libs3
#include "rpc/common.pb.h"
using huawei::proto::RESULT;
using std::string;

typedef struct s3_call_response{
    S3Status status;
    int size;
    int retries;
    int retrySleepInterval;
    int isTruncated;
    int keyCount;
    char nextMarker[1024];
    void *pdata;
}s3_call_response_t;

class CephS3Api {
private:
    S3BucketContext bucketContext;
public:
    CephS3Api(const char* access_key, const char* secret_key, const char* host,
            const char* bucket_name);
    ~CephS3Api();
    RESULT create_bucket_if_not_exists(const char* bucket_name);
    RESULT put_object(const char* obj_name, const string* value);
    RESULT delete_object(const char* key);
    RESULT list_objects(const char*prefix, const char*marker, int maxkeys, std::list<string>* list);
    RESULT get_object(const char* key, string* value);
};
#endif
