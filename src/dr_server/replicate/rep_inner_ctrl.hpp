/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_inner_ctrl.hpp
* Author: 
* Date:         2016/12/16
* Version:      1.0
* Description: provides control api to sg client
* 
************************************************/
#ifndef REP_INNER_CTRL_H_
#define REP_INNER_CTRL_H_
#include "replicate.h"
#include "../ceph_s3_meta.h"
#include "rpc/replicate_inner_control.grpc.pb.h"
using huawei::proto::inner::CreateReplicationInnerReq;
using huawei::proto::inner::EnableReplicationInnerReq;
using huawei::proto::inner::DisableReplicationInnerReq;
using huawei::proto::inner::FailoverReplicationInnerReq;
using huawei::proto::inner::ReverseReplicationInnerReq;
using huawei::proto::inner::QueryReplicationInnerReq;
using huawei::proto::inner::DeleteReplicationInnerReq;
using huawei::proto::inner::ListReplicationInnerReq;
using huawei::proto::inner::ReplicationInnerCommonRes;
using huawei::proto::inner::QueryReplicationInnerRes;
using huawei::proto::inner::ListReplicationInnerRes;
using huawei::proto::REP_STATUS;
using grpc::ClientContext;
// TODO:implement
class RepInnerCtrl:public huawei::proto::inner::ReplicateInnerControl::Service{
    //rpc replicate controls
    grpc::Status CreateReplication(ClientContext* context,
            const CreateReplicationInnerReq& request,
            ReplicationInnerCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status EnableReplication(ClientContext* context,
            const EnableReplicationInnerReq& request,
            ReplicationInnerCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status DisableReplication(ClientContext* context,
            const DisableReplicationInnerReq& request,
            ReplicationInnerCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status FailoverReplication(ClientContext* context,
            const FailoverReplicationInnerReq& request,
            ReplicationInnerCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status ReverseReplication(ClientContext* context,
            const ReverseReplicationInnerReq& request,
            ReplicationInnerCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status QueryReplication(ClientContext* context,
            const QueryReplicationInnerReq& request,
            QueryReplicationInnerRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status ListReplication(ClientContext* context,
            const ListReplicationInnerReq& request,
            ListReplicationInnerRes* response){
        return grpc::Status::OK;
    }
    grpc::Status DeleteReplication(ClientContext* context,
            const DeleteReplicationInnerReq& request,
            ReplicationInnerCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
public:
    RepInnerCtrl(Replicate& rep,
        std::shared_ptr<CephS3Meta> meta):
        replicate_(rep),meta_(meta){
    }
    ~RepInnerCtrl(){
    }
private:
    Replicate& replicate_;
    std::shared_ptr<CephS3Meta> meta_;
    void notify_replication_state(const string& vol,
        const REP_STATUS& state){
    }
};
#endif
