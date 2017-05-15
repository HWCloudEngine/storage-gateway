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
#include "common/libs3.h" // require ceph-libs3
#include "kv_api.h"
#define BUFF_LEN 128
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
    void *meta_data;
}s3_call_response_t;

class CephS3Api:public KVApi {
private:
    S3BucketContext bucketContext;
    char access_key_[BUFF_LEN];
    char secret_key_[BUFF_LEN];
    char host_[BUFF_LEN];
    char bucket_[BUFF_LEN];
public:
    CephS3Api(const char* access_key, const char* secret_key, const char* host,
            const char* bucket_name);
    ~CephS3Api();

    StatusCode create_bucket_if_not_exists(const char* bucket_name);
    StatusCode put_object(const char* obj_name, const string* value,
            const std::map<string,string>* meta=nullptr);
    StatusCode delete_object(const char* key);
    StatusCode list_objects(const char*prefix, const char*marker,
            int maxkeys, std::list<string>* list, const char* delimiter=nullptr);
    StatusCode get_object(const char* key, string* value);
    StatusCode head_object(const char* key,
            std::map<string,string>* meta=nullptr);
};
#endif
