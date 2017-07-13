/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_replay.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  replay io background
*
*************************************************/
#ifndef SRC_SG_CLIENT_JOURNAL_REPLAYER_H_
#define SRC_SG_CLIENT_JOURNAL_REPLAYER_H_
#include <string>
#include <boost/thread/thread.hpp>
#include "common/journal_entry.h"
#include "common/volume_attr.h"
#include "common/config_option.h"
#include "common/env_posix.h"
#include "rpc/clients/rpc_client.h"
#include "rpc/common.pb.h"
#include "rpc/message.pb.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"
#include "cache/cache_recover.h"
#include "snapshot/snapshot_proxy.h"
#include "backup/backup_decorator.h"
#include "replicate_proxy.h"

using google::protobuf::Message;
using huawei::proto::WriteMessage;
using huawei::proto::VolumeInfo;

class JournalReplayer {
 public:
    explicit JournalReplayer(VolumeAttr& vol_attr);
    ~JournalReplayer();
    JournalReplayer(const JournalReplayer& r) = delete;
    JournalReplayer& operator=(const JournalReplayer& r) = delete;

    bool init(shared_ptr<IDGenerator> id_maker_ptr,
              shared_ptr<CacheProxy> cache_proxy_ptr,
              shared_ptr<SnapshotProxy> snapshot_proxy_ptr,
              std::shared_ptr<ReplicateProxy> rep_proxy_ptr);
    bool deinit();

 private:
    /*replay thread work function*/
    void replay_volume_loop();
    /*update marker thread work function*/
    void update_marker_loop();
    /*replay only slave is replicating*/
    void replica_replay();
    /*replay both master and failover on slave*/
    bool normal_replay();
    bool replay_each_journal(const std::string& journal, const off_t& start_pos,
                             const off_t& end_pos);
    bool handle_io_cmd(shared_ptr<JournalEntry> entry);
    bool handle_ctrl_cmd(shared_ptr<JournalEntry> entry);
    void handle_snapshot_cmd(int type, SnapReqHead shead, std::string snap_name);
    void handle_backup_cmd(int type, SnapReqHead shead, std::string snap_name);
    void handle_replication_cmd(int type, SnapReqHead shead, std::string snap_name);
    void handle_replication_failover_cmd(int type, SnapReqHead shead, std::string snap_name);

    bool process_journal_entry(shared_ptr<JournalEntry> entry);
    /*entry from memory*/
    bool process_memory(shared_ptr<JournalEntry> entry);
    /*entry from journal file*/
    bool process_file(shared_ptr<CEntry> entry);
    void update_consumer_marker(const std::string& journal, const off_t& off);

 private:
    /*volume attr*/
    VolumeAttr& vol_attr_;
    /*block device write fd*/
    unique_ptr<AccessFile> blk_file_;
    /*consumer marker*/
    std::mutex journal_marker_mutex_;
    JournalMarker journal_marker_;
    bool update_;
    /*replay thread*/
    std::atomic_bool  running_;
    std::unique_ptr<boost::thread> replay_thread_ptr_;
    /*update mark thread*/
    std::unique_ptr<boost::thread> update_thread_ptr_;
    /*cache for replay*/
    std::shared_ptr<CacheProxy> cache_proxy_ptr_;
    /*cache recover when crash*/
    std::shared_ptr<IDGenerator> id_maker_ptr_;
    std::shared_ptr<CacheRecovery> cache_recover_ptr_;
    /*snapshot*/
    std::shared_ptr<SnapshotProxy> snapshot_proxy_ptr_;
    /*backup decorator*/
    std::shared_ptr<BackupDecorator> backup_decorator_ptr_;
    /*replicate proxy*/
    std::shared_ptr<ReplicateProxy> rep_proxy_ptr_;
};

#endif  // SRC_SG_CLIENT_JOURNAL_REPLAYER_H_
