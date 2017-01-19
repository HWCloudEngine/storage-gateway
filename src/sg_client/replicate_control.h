/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicate_control.h
* Author: 
* Date:         2017/01/11
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REPLICATE_CONTROL_H_
#define REPLICATE_CONTROL_H_
#include <memory>
#include "../rpc/clients/replicate_inner_ctrl_client.h"
#include "../rpc/replicate_control.grpc.pb.h"
#include "snapshot_control.h"
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::control::CreateReplicationReq;
using huawei::proto::control::ReplicationCommonRes;
using huawei::proto::control::EnableReplicationReq;
using huawei::proto::control::DisableReplicationReq;
using huawei::proto::control::FailoverReplicationReq;
using huawei::proto::control::ReverseReplicationReq;
using huawei::proto::control::DeleteReplicationReq;

class ReplicateCtrl:public huawei::proto::control::ReplicateControl::Service{
    Status CreateReplication(ServerContext* context,
                                    const CreateReplicationReq* request,
                                    ReplicationCommonRes* response);
    Status EnableReplication(ServerContext* context,
                                    const EnableReplicationReq* request,
                                    ReplicationCommonRes* response);
    Status DisableReplication(ServerContext* context,
                                     const DisableReplicationReq* request,
                                     ReplicationCommonRes* response);
    Status FailoverReplication(ServerContext* context,
                                      const FailoverReplicationReq* request,
                                      ReplicationCommonRes* response);
    Status ReverseReplication(ServerContext* context,
                                     const ReverseReplicationReq* request,
                                     ReplicationCommonRes* response);
    Status DeleteReplication(ServerContext* context,
                                    const DeleteReplicationReq* request,
                                    ReplicationCommonRes* response);
public:
    // TODO: use snap proxy
    ReplicateCtrl(SnapshotControlImpl* snap_ctrl_);
    ~ReplicateCtrl();
private:
    unique_ptr<ReplicateCtrlClient> rep_ctrl_client_;
    SnapshotControlImpl* snap_ctrl_;
};

#endif
