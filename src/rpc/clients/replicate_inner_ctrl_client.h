/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicate_ctrl_client.hpp
* Author: 
* Date:         2016/12/16
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REPLICATE_CTRL_CLIENT_H_
#define REPLICATE_CTRL_CLIENT_H_
#include <memory>
#include <string>
#include <grpc++/grpc++.h>
#include "../replicate_inner_control.grpc.pb.h"
using std::string;
using grpc::ClientContext;
using huawei::proto::REP_ROLE;
using huawei::proto::REP_SECONDARY;
using huawei::proto::inner::CreateReplicationInnerReq;
using huawei::proto::inner::EnableReplicationInnerReq;
using huawei::proto::inner::DisableReplicationInnerReq;
using huawei::proto::inner::FailoverReplicationInnerReq;
using huawei::proto::inner::ReverseReplicationInnerReq;
using huawei::proto::inner::DeleteReplicationInnerReq;
using huawei::proto::inner::ReplicationInnerCommonRes;
using huawei::proto::JournalMarker;
class ReplicateCtrlClient{
public:
    ReplicateCtrlClient(std::shared_ptr<grpc::Channel> channel):
        stub_(huawei::proto::inner::ReplicateInnerControl::NewStub(channel)){
    }
    ~ReplicateCtrlClient(){};
    bool create_replication(const string& op_id,const string& rep_id,
        const string& local_vol, const string& peer_vol,const REP_ROLE& role){
        CreateReplicationInnerReq req;
        req.set_operate_id(op_id);
        req.set_local_volume(local_vol);
        req.set_peer_volume(peer_vol);
        req.set_rep_uuid(rep_id);
        req.set_role(role);
        ClientContext context;
        ReplicationInnerCommonRes res;
        grpc::Status status = stub_->CreateReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool enable_replication(const string& op_id,const string& vol_id,
        const REP_ROLE& role,const JournalMarker& marker){
        EnableReplicationInnerReq req;
        req.set_operate_id(op_id);
        req.set_vol_id(vol_id);
        req.set_role(role);
        req.mutable_marker()->CopyFrom(marker);
        ClientContext context;
        ReplicationInnerCommonRes res;
        grpc::Status status = stub_->EnableReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool disable_replication(const string& op_id,const string& vol_id,
        const REP_ROLE& role,const JournalMarker& marker){
        DisableReplicationInnerReq req;
        req.set_operate_id(op_id);
        req.set_vol_id(vol_id);
        req.set_role(role);
        req.mutable_marker()->CopyFrom(marker);
        ClientContext context;
        ReplicationInnerCommonRes res;
        grpc::Status status = stub_->DisableReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool failover_replication(const string& op_id,const string& vol_id,
        const REP_ROLE& role,const JournalMarker& marker){
        FailoverReplicationInnerReq req;
        req.set_operate_id(op_id);
        req.set_vol_id(vol_id);
        req.set_role(role);
        req.mutable_marker()->CopyFrom(marker);
        ClientContext context;
        ReplicationInnerCommonRes res;
        grpc::Status status = stub_->FailoverReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool reverse_replication(const string& op_id,const string& vol_id,
        const REP_ROLE& role,const JournalMarker& marker){
        ReverseReplicationInnerReq req;
        req.set_operate_id(op_id);
        req.set_vol_id(vol_id);
        req.set_role(role);
        req.mutable_marker()->CopyFrom(marker);
        ClientContext context;
        ReplicationInnerCommonRes res;
        grpc::Status status = stub_->ReverseReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool delete_replication(const string& op_id,const string& vol_id,
        const REP_ROLE& role){
        DeleteReplicationInnerReq req;
        req.set_operate_id(op_id);
        req.set_vol_id(vol_id);
        req.set_role(role);
        ClientContext context;
        ReplicationInnerCommonRes res;
        grpc::Status status = stub_->DeleteReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    
private:
    std::unique_ptr<huawei::proto::inner::ReplicateInnerControl::Stub> stub_;
};
#endif
