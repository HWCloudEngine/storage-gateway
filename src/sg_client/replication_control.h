/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replication_control.h
* Author: 
* Date:         2017/01/11
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REPLICATION_CONTROL_H_
#define REPLICATION_CONTROL_H_
#include <memory>
#include "../rpc/clients/replicate_ctrl_client.h"
#include "../rpc/replicate_control.grpc.pb.h"
#include "control_service.h"
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::CreateReplicationReq;
using huawei::proto::ReplicationCommonRes;
using huawei::proto::EnableReplicationReq;
using huawei::proto::DisableReplicationReq;
using huawei::proto::FailoverReplicationReq;
using huawei::proto::ReverseReplicationReq;
using huawei::proto::DeleteReplicationReq;

class ReplicationCtrl:public huawei::proto::ReplicateControl::Service{
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
    ReplicationCtrl(ControlService* snap_ctrl_);
    ~ReplicationCtrl();
private:
    unique_ptr<ReplicateCtrlClient> rep_ctrl_client_;
    ControlService* snap_ctrl_;
};

#endif
