/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_mgr.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot request dispatch
* 
*************************************************/
#ifndef SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_MGR_H_
#define SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_MGR_H_
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/snapshot_inner_control.grpc.pb.h"
#include "snapshot_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::StatusCode;
using huawei::proto::inner::SnapshotInnerControl;
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
using huawei::proto::SnapStatus;
using huawei::proto::DiffBlocks;

/*work on storage gateway server, all snapshot api gateway */
class SnapshotMgr : public SnapshotInnerControl::Service {
 private:
    SnapshotMgr();
    ~SnapshotMgr();

 public:
    static SnapshotMgr& singleton();

    /*call by volume ctrl service when add and delete volume*/
    StatusCode add_volume(const std::string& vol_name, const size_t& vol_size);
    StatusCode del_volume(const std::string& vol_name);

    /*grpc interface*/
    grpc::Status Sync(ServerContext* context, const SyncReq* req, SyncAck* ack) override;
    grpc::Status Create(ServerContext* context, const CreateReq* req, CreateAck* ack) override;
    grpc::Status List(ServerContext* context, const ListReq* req, ListAck* ack) override;
    grpc::Status Query(ServerContext* context, const QueryReq* req, QueryAck* ack) override;
    grpc::Status Delete(ServerContext* context, const DeleteReq* req, DeleteAck* ack) override;
    grpc::Status Rollback(ServerContext* context, const RollbackReq* req, RollbackAck* ack) override;
    grpc::Status Update(ServerContext* context, const UpdateReq* req, UpdateAck* ack) override;
    grpc::Status CowOp(ServerContext* context, const CowReq* req, CowAck* ack) override;
    grpc::Status CowUpdate(ServerContext* context, const CowUpdateReq* req, CowUpdateAck* ack) override;
    grpc::Status Diff(ServerContext* context, const DiffReq* req, DiffAck* ack) override;
    grpc::Status Read(ServerContext* context, const ReadReq* req, ReadAck* ack) override;

    /*local interface*/
    StatusCode Query(const std::string& vol_name, const std::string& snap_name, SnapStatus& snap_status);
    StatusCode Diff(const std::string& vol_name, const std::string& check_point, 
                    const std::string& first_snap, const std::string& last_snap,  std::vector<DiffBlocks>& blocks);

 private:
    std::mutex m_mutex;
    /*each volume own a snapshot mds*/
    std::map<std::string, std::shared_ptr<SnapshotMds>> m_all_snapmds;
};

#endif  // SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_MGR_H_
