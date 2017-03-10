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
#include <mutex>
#include <boost/thread/shared_mutex.hpp>
#include "common/libs3.h" // require ceph-libs3
#include "journal_meta_manager.h"
#include "journal_gc_manager.h"
#include "volume_meta_manager.h"
#include "common/ceph_s3_api.h"
#include "lru_cache.h"
using huawei::proto::JournalMeta;
using huawei::proto::JournalArray;
using google::protobuf::int64;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeInfo;

// sub counter occupys Least 16 bits(binary), which used for synced snapshot
#define SUB_COUNTER_BITS (16)
// decimal bits of printed counter in journal key
#define PRINT_COUNTER_BITS (012)
const string g_key_prefix = "/journals/";
const string g_marker_prefix = "/markers/";
const string g_writer_prefix = "/session/writer/";
const string g_sealed = "/sealed/";
const string g_replayer = "replayer";
const string g_replicator = "replicator";
const string g_recycled = "/recycled/";
const string g_consumer = "consumer/";
const string g_producer = "producer/";
const string g_volume_prefix = "/volumes/";

// struct that contains head/tail counter of journals in a volume
typedef struct JournalHeadTail{
    uint64_t head;
    uint64_t tail;
    std::mutex mtx;
}JournalHeadTail;

class CephS3Meta:public JournalMetaManager,
        public JournalGCManager,
        public VolumeMetaManager {
private:
    std::unique_ptr<CephS3Api> s3Api_ptr_;
    string mount_path_;
    boost::shared_mutex counter_mtx;
    // volume journal_name counters
    std::map<string,std::shared_ptr<JournalHeadTail>> counter_map_;
    // journal meta cache
    LruCache<string,JournalMeta> journal_meta_cache_;
    // produer marker cache
    LruCache<string,JournalMarker> replayer_Pmarker_cache_;
    LruCache<string,JournalMarker> replicator_Pmarker_cache_;
    // consumer marker cache
    LruCache<string,JournalMarker> replayer_Cmarker_cache_;
    LruCache<string,JournalMarker> replicator_Cmarker_cache_;
    // volume cache
    LruCache<string,VolumeMeta> vol_cache_;
    // max journal size
    int max_journal_size_;
    std::shared_ptr<JournalHeadTail> init_journal_key_counter(
                            const string& vol_id);
    std::shared_ptr<JournalHeadTail> get_journal_key_counter(
                            const string& vol_id);

    void sort_journals(const string& vol_id, std::list<string>& list);

    int compare_journal_key(const string& vol_id,
                const string& key1,const string& key2);

    bool get_marker(const string& vol_id,
        const CONSUMER_TYPE& type, JournalMarker& marker,bool is_consumer);
    RESULT init();
    RESULT create_journal_file(const string& name);
    // default get function for LruCaches
    bool _get_journal_meta(const string& key, JournalMeta& meta);
    bool _get_replayer_producer_marker(const string& key,
            JournalMarker& marker);
    bool  _get_replayer_consumer_marker(const string& key,
            JournalMarker& marker);
    bool _get_replicator_producer_marker(const string& key,
            JournalMarker& marker);
    bool _get_replicator_consumer_marker(const string& key,
            JournalMarker& marker);
    bool _get_volume_meta(const string& key,VolumeMeta& info);
public:
    CephS3Meta();
    ~CephS3Meta();

    virtual int compare_marker(const string& vol_id,
                const JournalMarker& m1, const JournalMarker& m2);

    virtual RESULT get_journal_meta(const string& key, JournalMeta& meta);
    virtual RESULT get_producer_marker(const string& vol_id,
            const CONSUMER_TYPE& type, JournalMarker& marker);
    // journal meta management
    virtual RESULT create_journals(const string& uuid,const string& vol_id,
            const int& limit, std::list<string> &list);
    virtual RESULT create_journals_by_given_keys(const string& uuid,
            const string& vol_id,const std::list<string> &list);
    virtual RESULT seal_volume_journals(const string& uuid,
            const string& vol_id,
            const string journals[],const int& count);
    virtual RESULT get_consumer_marker(const string& vol_id,
            const CONSUMER_TYPE& type,JournalMarker& marker);
    virtual RESULT update_consumer_marker(const string& vol_id,
            const CONSUMER_TYPE& type,const JournalMarker& marker);
    virtual RESULT get_consumable_journals(const string& vol_id,
            const JournalMarker& marker,const int& limit,
            std::list<JournalElement> &list,
            const CONSUMER_TYPE& type);
    virtual RESULT set_producer_marker(const string& vol_id,
            const JournalMarker& marker);
    // gc management
    virtual RESULT get_sealed_and_consumed_journals(
            const string& vol_id, const JournalMarker& marker,const int& limit,
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
