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
#include "../replicate_control.grpc.pb.h"
using std::string;
using ::grpc::Status;
using ::grpc::ClientContext;
using ::huawei::proto::REP_ROLE;
using ::huawei::proto::REP_SECONDARY;
using ::huawei::proto::ReplicationCommonReq;
using ::huawei::proto::ReplicationCommonRes;
using ::huawei::proto::CreateReplicationReq;
using ::huawei::proto::QueryReplicationRes;
using ::huawei::proto::ListReplicationReq;
using ::huawei::proto::ListReplicationRes;
using ::huawei::proto::JournalMarker;
class ReplicateCtrlClient{
public:
    ReplicateCtrlClient(std::shared_ptr<Channel> channel):
        stub_(huawei::proto::ReplicateControl::NewStub(channel)){
    }
    ~ReplicateCtrlClient();
    // TODO: implement
    bool create_replication(const string& op_id,const string& rep_id,
        const string& primary, const string& secondary,REP_ROLE role){
        CreateReplicationReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        req.set_primary_volume(primary);
        req.set_secondary_volume(secondary);
        req.set_role(role);
        ClientContext context;
        ReplicationCommonRes res;
        Status status = stub_->CreateReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        
        return false;
    }
    bool enable_replication(const string& op_id,const string& rep_id,
        REP_ROLE role,const JournalMarker& marker){
        ReplicationCommonReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        req.set_role(role);
        req.mutable_marker()->CopyFrom(marker);
        ClientContext context;
        ReplicationCommonRes res;
        Status status = stub_->EnableReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool disable_replication(const string& op_id,const string& rep_id,
        REP_ROLE role,const JournalMarker& marker){
        ReplicationCommonReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        req.set_role(role);
        req.mutable_marker()->CopyFrom(marker);
        ClientContext context;
        ReplicationCommonRes res;
        Status status = stub_->DisableReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool failover_replication(const string& op_id,const string& rep_id,
        REP_ROLE role=REP_SECONDARY){
        ReplicationCommonReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        req.set_role(role);
        ClientContext context;
        ReplicationCommonRes res;
        Status status = stub_->FailoverReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool reverse_replication(const string& op_id,const string& rep_id,
        const string& primary, REP_ROLE role){
        ReplicationCommonReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        req.set_role(role);
        ClientContext context;
        ReplicationCommonRes res;
        Status status = stub_->ReverseReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    bool delete_replication(const string& op_id,const string& rep_id,
        REP_ROLE role){
        ReplicationCommonReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        req.set_role(role);
        ClientContext context;
        ReplicationCommonRes res;
        Status status = stub_->DeleteReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return true;
        return false;
    }
    QueryReplicationRes query_replication(const string& op_id,
        const string& rep_id){
        ReplicationCommonReq req;
        req.set_operate_id(op_id);
        req.set_uuid(rep_id);
        ClientContext context;
        QueryReplicationRes res;
        Status status = stub_->QueryReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return res;
        QueryReplicationRes res_;
        return res_;
    }
    ListReplicationRes list_replication(const string& op_id){
        ListReplicationReq req;
        req.set_operate_id(op_id);
        ClientContext context;
        ListReplicationRes res;
        Status status = stub_->ListReplication(&context,req,&res);
        if(status.ok() && !res.ret())
            return res;
        ListReplicationRes res_;
        return res_;
    }
    
private:
    std::unique_ptr<huawei::proto::ReplicateControl::Stub> stub_;
};
#endif
