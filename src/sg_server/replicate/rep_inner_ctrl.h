/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_inner_ctrl.h
* Author: 
* Date:         2016/12/16
* Version:      1.0
* Description: provides control api to sg client
* 
************************************************/
#ifndef REP_INNER_CTRL_H_
#define REP_INNER_CTRL_H_
#include "rep_scheduler.h"
#include "../ceph_s3_meta.h"
#include "rpc/replicate_inner_control.grpc.pb.h"
#include "common/thread_pool.h"
using huawei::proto::inner::CreateReplicationInnerReq;
using huawei::proto::inner::EnableReplicationInnerReq;
using huawei::proto::inner::DisableReplicationInnerReq;
using huawei::proto::inner::FailoverReplicationInnerReq;
using huawei::proto::inner::ReverseReplicationInnerReq;
using huawei::proto::inner::DeleteReplicationInnerReq;
using huawei::proto::inner::ReplicationInnerCommonRes;
using huawei::proto::inner::ReportCheckpointReq;
using huawei::proto::inner::ReportCheckpointRes;
using huawei::proto::REP_PRIMARY;
using huawei::proto::REP_SECONDARY;
using grpc::ClientContext;
using grpc::ServerContext;

class RepInnerCtrl:public huawei::proto::inner::ReplicateInnerControl::Service{
    //rpc replicate controls
    grpc::Status CreateReplication(ServerContext* context,
            const CreateReplicationInnerReq* request,
            ReplicationInnerCommonRes* response);
    grpc::Status EnableReplication(ServerContext* context,
            const EnableReplicationInnerReq* request,
            ReplicationInnerCommonRes* response);
    grpc::Status DisableReplication(ServerContext* context,
            const DisableReplicationInnerReq* request,
            ReplicationInnerCommonRes* response);
    grpc::Status FailoverReplication(ServerContext* context,
            const FailoverReplicationInnerReq* request,
            ReplicationInnerCommonRes* response);
    grpc::Status ReverseReplication(ClientContext* context,
            const ReverseReplicationInnerReq* request,
            ReplicationInnerCommonRes* response);
    grpc::Status DeleteReplication(ServerContext* context,
            const DeleteReplicationInnerReq* request,
            ReplicationInnerCommonRes* response);
    grpc::Status ReportCheckpoint(ServerContext* context,
            const ReportCheckpointReq* request,
            ReportCheckpointRes* response);
    void init();
    void notify_rep_state_changed(const string& vol);
    bool validate_replicate_operation(const RepStatus& status,
            const ReplicateOperation& op);
public:
    RepInnerCtrl(RepScheduler& rep,
            std::shared_ptr<CephS3Meta> meta):
            rep_(rep),meta_(meta),running_(true){
        init();
    }
    ~RepInnerCtrl(){
        running_ = false;
    }
private:
    RepScheduler& rep_;
    std::shared_ptr<CephS3Meta> meta_;
    bool running_;
};
#endif
