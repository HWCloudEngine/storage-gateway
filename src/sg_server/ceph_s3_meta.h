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
#include "volume_meta_manager.h"
#include "ceph_s3_api.h"
using huawei::proto::JournalMeta;
using huawei::proto::JournalArray;
using google::protobuf::int64;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeInfo;
class CephS3Meta:public JournalMetaManager,
        public JournalGCManager,
        public VolumeMetaManager {
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
    virtual RESULT get_producer_marker(const string& vol_id,
            const CONSUMER_TYPE& type, JournalMarker& marker);
    // journal meta management
    virtual RESULT create_journals(const string& uuid,const string& vol_id,
            const int& limit, std::list<string> &list);
    virtual RESULT create_journals_by_given_keys(const string& uuid,
            const string& vol_id,const std::list<string> &list);
    virtual RESULT seal_volume_journals(const string& uuid,const string& vol_id,
            const string journals[],const int& count);
    virtual RESULT get_consumer_marker(const string& vol_id,
            const CONSUMER_TYPE& type,JournalMarker& marker);
    virtual RESULT update_consumer_marker(const string& vol_id,
            const CONSUMER_TYPE& type,const JournalMarker& marker);
    virtual RESULT get_consumable_journals(const string& vol_id,
            const JournalMarker& marker,const int& limit, std::list<string> &list,
            const CONSUMER_TYPE& type);
    virtual RESULT set_producer_marker(const string& vol_id,
            const JournalMarker& marker);
    // gc management
    virtual RESULT get_sealed_and_consumed_journals(
            const string& vol_id, const CONSUMER_TYPE& type,const int& limit,
            std::list<string> &list);
    virtual RESULT recycle_journals(const string& vol_id,
            const std::list<string>& journals);
    virtual RESULT get_producer_id(const string& vol_id,
            std::list<string>& list);
    virtual RESULT seal_opened_journals(const string& vol_id,
            const string& uuid);
    virtual RESULT list_volumes(std::list<string>& list);
    // volume managment
    virtual RESULT list_volume_meta(std::list<VolumeMeta> &list);
    virtual RESULT read_volume_meta(const string& vol_id,VolumeMeta& meta);
    virtual RESULT update_volume_meta(const VolumeMeta& meta);
    virtual RESULT create_volume(const VolumeMeta& meta);
    virtual RESULT delete_volume(const string& vol_id);
};
#endif
