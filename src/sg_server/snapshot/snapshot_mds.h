/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_mds.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot meta management
* 
*************************************************/
#ifndef SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_MDS_H_
#define SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_MDS_H_
#include <string>
#include <mutex>
#include <map>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/snapshot_inner_control.pb.h"
#include "rpc/snapshot_inner_control.grpc.pb.h"
#include "log/log.h"
#include "common/block_store.h"
#include "common/index_store.h"
#include "common/define.h"
#include "snapshot_type.h"

using huawei::proto::StatusCode;
using huawei::proto::SnapStatus;
using huawei::proto::SnapType;
using huawei::proto::SnapReqHead;
using huawei::proto::inner::CreateReq;
using huawei::proto::inner::CreateAck;
using huawei::proto::inner::ListReq;
using huawei::proto::inner::ListAck;
using huawei::proto::inner::QueryReq;
using huawei::proto::inner::QueryAck;
using huawei::proto::inner::DeleteReq;
using huawei::proto::inner::DeleteAck;
using huawei::proto::inner::RollbackReq;
using huawei::proto::inner::RollbackAck;
using huawei::proto::inner::UpdateReq;
using huawei::proto::inner::UpdateAck;
using huawei::proto::inner::CowReq;
using huawei::proto::inner::CowAck;
using huawei::proto::inner::CowUpdateReq;
using huawei::proto::inner::CowUpdateAck;
using huawei::proto::inner::DiffReq;
using huawei::proto::inner::DiffAck;
using huawei::proto::inner::ReadReq;
using huawei::proto::inner::ReadAck;
using huawei::proto::inner::SyncReq;
using huawei::proto::inner::SyncAck;

class SnapshotMds {
 public:
    SnapshotMds(const std::string& vol_name, const size_t& vol_size);
    SnapshotMds(const SnapshotMds& other) = delete;
    SnapshotMds& operator=(const SnapshotMds& other) = delete;
    virtual ~SnapshotMds();

    /*storage client sync snapshot status*/
    StatusCode sync(const SyncReq* req, SyncAck* ack);
    /*snapshot common operation*/
    StatusCode create_snapshot(const CreateReq* req, CreateAck* ack);
    StatusCode delete_snapshot(const DeleteReq* req, DeleteAck* ack);
    StatusCode rollback_snapshot(const RollbackReq* req, RollbackAck* ack);
    StatusCode list_snapshot(const ListReq* req, ListAck* ack);
    StatusCode query_snapshot(const QueryReq* req, QueryAck* ack);
    StatusCode diff_snapshot(const DiffReq* req, DiffAck* ack);
    StatusCode read_snapshot(const ReadReq* req, ReadAck* ack);
    /*snapshot status*/
    StatusCode update(const UpdateReq* req, UpdateAck* ack);
    /*cow*/
    StatusCode cow_op(const CowReq* req, CowAck* ack);
    StatusCode cow_update(const CowUpdateReq* req, CowUpdateAck* ack);
    /*crash recover*/
    int recover();

 private:
    /*common helper*/
    snap_id_t get_prev_snap_id(const snap_id_t current);
    snap_id_t get_next_snap_id(const snap_id_t current);
    snap_id_t   get_snap_id(const std::string& snap_name);
    std::string get_snap_name(snap_id_t snap_id);
    std::string get_latest_snap_name();
    snap_id_t   get_latest_snap_id();
    bool check_snap_exist(const std::string& snap_name);
    /*accord local and remote to mapping snapshot pair*/
    std::string mapping_snap_name(const SnapReqHead& shead, const std::string& sname);
    /*maintain snapshot status*/
    StatusCode create_event_update_status(const std::string& snap_name);
    StatusCode delete_event_update_status(const std::string& snap_name);
    StatusCode rollback_event_update_status(const std::string& snap_name);
    /*helper to generate cow object name*/
    std::string spawn_block_url(const snap_id_t snap_id, const block_t blk_id);
    /*debug*/
    void trace();

 private:
    /*volume name*/
    std::string m_volume_name;
    size_t m_volume_size;
    /*the latest snapshot id*/
    snap_id_t m_latest_snapid;
    /*index store for snapshot meta persist*/
    IndexStore* m_index_store;
    /*block store for cow object*/
    BlockStore* m_block_store;
};

#endif  // SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_MDS_H_
