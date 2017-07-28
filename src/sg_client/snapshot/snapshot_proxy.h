/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    snapshot.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot interface
*  
*************************************************/
#ifndef SRC_SG_CLIENT_SNAPSHOT_SNAPSHOT_PROXY_H_
#define SRC_SG_CLIENT_SNAPSHOT_SNAPSHOT_PROXY_H_
#include <stdlib.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <condition_variable>
#include <atomic>
#include <grpc++/grpc++.h>
#include "log/log.h"
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/journal.pb.h"
#include "rpc/snapshot_control.pb.h"
#include "rpc/snapshot_control.grpc.pb.h"
#include "rpc/snapshot_inner_control.pb.h"
#include "rpc/snapshot_inner_control.grpc.pb.h"
#include "rpc/clients/rpc_client.h"
#include "common/define.h"
#include "common/blocking_queue.h"
#include "common/block_store.h"
#include "common/volume_attr.h"
#include "common/env_posix.h"
#include "common/define.h"
#include "common/interval_set.h"
#include "snapshot.h"
#include "syncbarrier.h"
#include "transaction.h"
#include "common/journal_entry.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::VolumeInfo;
using huawei::proto::StatusCode;
using huawei::proto::SnapType;
using huawei::proto::SnapStatus;
using huawei::proto::SnapReqHead;
using huawei::proto::JournalMarker;
using huawei::proto::DiffBlocks;
using huawei::proto::inner::SnapshotInnerControl;
using huawei::proto::inner::UpdateEvent;

/*split io into fixed size block*/
struct cow_block {
    off_t   off;
    size_t  len;
    block_t blk_no;
};
typedef struct cow_block cow_block_t;

/*work on storage gateway client, each volume own a SnapshotProxy*/
class SnapshotProxy : public ISnapshot, public ITransaction, public ISyncBarrier {
 public:
    SnapshotProxy(VolumeAttr& vol_attr, BlockingQueue<shared_ptr<JournalEntry>>& entry_queue);
    ~SnapshotProxy();

    bool init();
    bool fini();

    /*called by control layer*/
    StatusCode create_snapshot(const CreateSnapshotReq* req,
                               CreateSnapshotAck* ack, JournalMarker& marker);
    StatusCode create_snapshot(const CreateSnapshotReq* req,
                               CreateSnapshotAck* ack) override;
    StatusCode delete_snapshot(const DeleteSnapshotReq* req,
                               DeleteSnapshotAck* ack) override;
    StatusCode rollback_snapshot(const RollbackSnapshotReq* req,
                                 RollbackSnapshotAck* ack) override;
    StatusCode list_snapshot(const ListSnapshotReq* req,
                             ListSnapshotAck* ack)override;
    StatusCode query_snapshot(const QuerySnapshotReq* req,
                              QuerySnapshotAck* ack) override;
    StatusCode diff_snapshot(const DiffSnapshotReq* req,
                             DiffSnapshotAck* ack) override;
    StatusCode read_snapshot(const ReadSnapshotReq* req,
                             ReadSnapshotAck* ack) override;

    /*call by journal replayer*/
    StatusCode create_transaction(const SnapReqHead& shead,
                                  const std::string& snap_name) override;
    StatusCode delete_transaction(const SnapReqHead& shead,
                                  const std::string& snap_name) override;
    StatusCode rollback_transaction(const SnapReqHead& shead,
                                    const std::string& snap_name) override;
    StatusCode cow_op(const off_t& off, const size_t& size, char* buf, bool rollback);
    StatusCode update(const SnapReqHead& shead, const std::string& sname,
                      const UpdateEvent& sevent);
    bool check_exist_snapshot()const;

    /*make sure journal writer persist ok then ack to client*/
    void cmd_persist_wait();
    void cmd_persist_notify(const JournalMarker& mark);

    void add_sync(const std::string& actor, const std::string& action) override;
    void del_sync(const std::string& actor) override;
    bool check_sync_on(const std::string& actor) override;

 private:
   void split_cow_block(const off_t& off, const size_t& size, vector<cow_block_t>& cow_blocks);

    /*accord message type to spawn journal entry*/
    shared_ptr<JournalEntry> spawn_journal_entry(const SnapReqHead& shead,
                                                 const std::string& sname,
                                                 const journal_event_type_t& entry_type);

    /*common transaction mechanism*/
    StatusCode transaction(const SnapReqHead& shead, const std::string& sname,
                           const UpdateEvent& sevent);

    size_t read_from_block_store(const off_t off, const size_t len,
                                 const std::vector<ReadBlock>& blocks, char* buf);
    size_t read_from_block_device(const off_t off, const size_t len,
                                  const interval_set<uint64_t>& read_blkdev_area,
                                  char* buf);
    interval_set<uint64_t> cal_read_blkdev_area(const off_t off, const size_t len,
                                                const std::vector<ReadBlock>& blocks);
    void dedup_cow_block(const std::vector<ReadBlock>& prev_cow_blocks,
                         std::vector<ReadBlock>& cur_cow_blocks);

 private:
    /*volume attr*/
    VolumeAttr& m_vol_attr;
    /*block device read write fd*/
    unique_ptr<AccessFile> m_block_file;
    /*entry queue to journal preprocessor*/
    BlockingQueue<shared_ptr<JournalEntry>>& m_entry_queue;
    /*sync between writer and proxy*/
    mutex m_cmd_persist_lock;
    condition_variable m_cmd_persist_cond;
    std::atomic_bool m_cmd_persist_ok{false};
    JournalMarker m_cmd_persist_mark;
    /*sync table*/
    map<std::string, std::string> m_sync_table;
    /*current active snapshot*/
    std::string  m_active_snapshot;
    /*check now exist snapshot or not*/
    atomic_bool m_exist_snapshot{false};
    /*snapshot block store*/
    BlockStore* m_block_store;
};

#endif  // SRC_SG_CLIENT_SNAPSHOT_SNAPSHOT_PROXY_H_
