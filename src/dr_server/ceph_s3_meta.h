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
#include <memory>
#include <thread>
#include "libs3.h" // require ceph-libs3
#include "journal_meta_manager.h"
#include "ceph_s3_api.h"
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

class CephS3Meta:public JournalMetaManager {
private:    
    std::unique_ptr<CephS3Api> _s3Api_ptr;
    std::thread _thread_GC;
    string _mount_path;
    std::map<string,int64_t> _map; // volume journal_name counters
    RESULT init_journal_name_counter(const string vol_id,int64_t& cnt);
    RESULT get_journal_name_counter(const string vol_id,int64_t& cnt);
    RESULT add_journal_name_counter(const string vol_id,const int64_t cnt);
public:
    static bool _thread_GC_running;
    CephS3Meta();
    ~CephS3Meta();
    RESULT init(const char* access_key, const char* secret_key, const char* host,
            const char* bucket_name, const char* path);
    RESULT update_journals_meta(string vol_id, string journals[],
        int count, JOURNAL_STATUS status);
    void init_GC_thread();
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
