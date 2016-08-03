/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    ceph_s3_meta.h
* Author: 
* Date:         2016/07/11
* Version:      1.0
* Description:
* 
************************************************/
#ifndef CEPH_S3_META_H_
#define CEPH_S3_META_H_
#include <cstdint>
#include "libs3.h" // require ceph-libs3
#include "journal_meta_service.h"
using huawei::proto::JournalMeta;
using huawei::proto::JournalArray;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::OPENED;
using huawei::proto::SEALED;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATER;
using huawei::proto::JournalIDCounters;
using google::protobuf::int64;

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

class CephS3Meta:public JournalMetaService {
private:
    S3BucketContext bucketContext;
    JournalIDCounters counters;
    int counters_updates;
    string _mount_path;
public:
    CephS3Meta();
    ~CephS3Meta();
    RESULT create_bucket_if_not_exists(const char* bucket_name);
    void put_object(const char* obj_name, s3_call_response_t& response);
    void delete_object(const char* key,s3_call_response_t& response);
    void list_objects(const char*prefix, const char*marker, int maxkeys, s3_call_response_t& response);
    void get_object(const char* key, s3_call_response_t& response);
    RESULT init(const char* access_key, const char* secret_key, const char* host,
            const char* bucket_name, const char* path);
    RESULT update_journals_meta(string vol_id, string journals[],
        int count, JOURNAL_STATUS status);
    RESULT restore_volume_journalsID_counter();
    RESULT update_volume_journalsID_counter();
    RESULT get_volume_journalsID_counter(string vol_id, int64& cnt);
    
    virtual RESULT get_volume_journals(string vol_id,int limit, std::list<string> &list);
    virtual RESULT delete_journals(string vol_id, string journals[], int count);
    virtual RESULT seal_volume_journals(string vol_id, string journals[],
            int count);
    virtual RESULT get_journal_marker(string vol_id, CONSUMER_TYPE type,
            JournalMarker* marker);
    virtual RESULT update_journals_marker(string vol_id, CONSUMER_TYPE type,
            JournalMarker marker);
    virtual RESULT get_consumer_journals(string vol_id, JournalMarker marker,
            int limit, std::list<string> &list);
};
#endif
