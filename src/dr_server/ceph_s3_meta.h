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
using huawei::proto::NO_SUCH_KEY;
using huawei::proto::OPENED;
using huawei::proto::SEALED;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATER;
using google::protobuf::int64;

class CephS3Meta:public JournalMetaManager {
private:    
    std::shared_ptr<CephS3Api> s3Api_ptr_;
    std::thread thread_GC_;
    string mount_path_;
    std::map<string,int64_t> counter_map_; // volume journal_name counters
    std::map<string,JournalMeta> kv_map_; // major key value cache
    RESULT init_journal_key_counter(const string& vol_id,int64_t& cnt);
    RESULT get_journal_key_counter(const string& vol_id,int64_t& cnt);
    RESULT add_journal_key_counter(const string& vol_id,const int64_t& cnt);
    RESULT get_journal_meta_by_key(const string& key, JournalMeta& meta);
    RESULT mark_journal_recycled(const string& uuid, const string& vol_id, const string& key);
public:
    static bool thread_GC_running_;
    CephS3Meta();
    ~CephS3Meta();
    RESULT init(const char* access_key, const char* secret_key, const char* host,
            const char* bucket_name, const char* path);
    void init_GC_thread();
    virtual RESULT get_volume_journals(const string& uuid,const string& vol_id,
            const int& limit, std::list<string> &list);
    virtual RESULT seal_volume_journals(const string& uuid,const string& vol_id,
            const string journals[],const int& count);
    virtual RESULT get_journal_marker(const string& uuid,const string& vol_id,
            const CONSUMER_TYPE& type,JournalMarker* marker);
    virtual RESULT update_journals_marker(const string& uuid, const string& vol_id,
            const CONSUMER_TYPE& type,const JournalMarker& marker);
    virtual RESULT get_consumer_journals(const string& uuid,const string& vol_id,
            const JournalMarker& marker,const int& limit, std::list<string> &list);
};
#endif
