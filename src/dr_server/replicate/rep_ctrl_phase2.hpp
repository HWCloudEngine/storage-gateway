/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_ctrl_phase2.hpp
* Author: 
* Date:         2016/12/16
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_CTRL_PHASE2_H_
#define REP_CTRL_PHASE2_H_
#include "replicate.h"
#include "../ceph_s3_meta.h"
#include "rpc/replicate_control.grpc.pb.h"
using ::huawei::proto::ReplicationCommonReq;
using ::huawei::proto::ReplicationCommonRes;
using ::huawei::proto::CreateReplicationReq;
using ::huawei::proto::QueryReplicationRes;
using ::huawei::proto::ListReplicationReq;
using ::huawei::proto::ListReplicationRes;
using ::huawei::proto::REP_STATUS;
using ::grpc::ClientContext;
// TODO:implement
class RepCtrlPhase2:public huawei::proto::ReplicateControl::Service{
    //rpc replicate controls
    grpc::Status CreateReplication(ClientContext* context,
            const CreateReplicationReq& request, ReplicationCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status EnableReplication(ClientContext* context,
            const ReplicationCommonReq& request, ReplicationCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status DisableReplication(ClientContext* context,
            const ReplicationCommonReq& request, ReplicationCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status FailoverReplication(ClientContext* context,
            const ReplicationCommonReq& request, ReplicationCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status ReverseReplication(ClientContext* context,
            const ReplicationCommonReq& request, ReplicationCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status QueryReplication(ClientContext* context,
            const ReplicationCommonReq& request, QueryReplicationRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
    grpc::Status ListReplication(ClientContext* context,
            const ListReplicationReq& request, ListReplicationRes* response){
        return grpc::Status::OK;
    }
    grpc::Status DeleteReplication(ClientContext* context,
            const ReplicationCommonReq& request, ReplicationCommonRes* response){
        grpc::Status status(grpc::INTERNAL,"not implement.");
        return status;
    }
public:
    RepCtrlPhase2(Replicate& rep,
        std::shared_ptr<CephS3Meta> meta):
        replicate_(rep),meta_(meta){
    }
    ~RepCtrlPhase2(){
    }
private:
    Replicate& replicate_;
    std::shared_ptr<CephS3Meta> meta_;
    void notify_replication_state(const string& vol,
        const REP_STATUS& state){
    }
};
#endif
