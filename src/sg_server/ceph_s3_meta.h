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
#include <atomic>
#include "common/libs3.h" // require ceph-libs3
#include "journal_meta_manager.h"
#include "journal_gc_manager.h"
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

class CephS3Meta:public JournalMetaManager,public JournalGCManager{
private:    
    std::unique_ptr<CephS3Api> s3Api_ptr_;
    string mount_path_;
    std::map<string,std::shared_ptr<std::atomic<int64_t>>> counter_map_; // volume journal_name counters
    std::map<string,JournalMeta> kv_map_; // major key value cache
    RESULT init_journal_key_counter(const string& vol_id,int64_t& cnt);
    RESULT get_journal_key_counter(const string& vol_id,int64_t& cnt);
    RESULT set_journal_key_counter(const string& vol_id,
            int64_t& expected,const int64_t& val);
    RESULT get_journal_meta(const string& key, JournalMeta& meta);
    RESULT init();
public:
    CephS3Meta();
    ~CephS3Meta();
    virtual RESULT create_journals(const string& uuid,const string& vol_id,
            const int& limit, std::list<string> &list);
    virtual RESULT create_journals_by_given_keys(const string& uuid,
            const string& vol_id,const std::list<string> &list);
    virtual RESULT seal_volume_journals(const string& uuid,const string& vol_id,
            const string journals[],const int& count);
    virtual RESULT get_journal_marker(const string& uuid,const string& vol_id,
            const CONSUMER_TYPE& type,JournalMarker* marker,
            const bool is_consumer=true);
    virtual RESULT update_journal_marker(const string& uuid, const string& vol_id,
            const CONSUMER_TYPE& type,const JournalMarker& marker,
            const bool is_consumer=true);
    virtual RESULT get_consumable_journals(const string& uuid,const string& vol_id,
            const JournalMarker& marker,const int& limit, std::list<string> &list);
    // gc manager
    virtual RESULT get_sealed_and_consumed_journals(const string& vol_id,
            const int& limit, std::list<string> &list);
    virtual RESULT recycle_journals(const string& vol_id,
            const std::list<string>& journals);
    virtual RESULT get_recycled_journals(const string& vol_id,
            const int& limit, std::list<string>& list);
    virtual RESULT delete_journals(const string& vol_id,
            const std::list<string>& journals);
    virtual RESULT get_producer_id(const string& vol_id,
            std::list<string>& list);
    virtual RESULT seal_opened_journals(const string& vol_id,
            const string& uuid);
    virtual RESULT list_volumes(std::list<string>& list);
};
#endif
