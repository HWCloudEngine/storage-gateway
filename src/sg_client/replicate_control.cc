/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replication_control.cc
* Author: 
* Date:         2017/01/11
* Version:      1.0
* Description:
* 
************************************************/
#include <string>
#include "replicate_control.h"
#include "common/config_parser.h"
#include "log/log.h"
using std::string;
ReplicateCtrl::ReplicateCtrl(SnapshotControlImpl* snap_ctrl):
        snap_ctrl_(snap_ctrl){
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    string default_ip("127.0.0.1");
    string svr_ip = parser->get_default("meta_server.ip",default_ip);
    int svr_port = parser->get_default("meta_server.port",50051);
    svr_ip += ":" + std::to_string(svr_port);
    rep_ctrl_client_.reset(new ReplicateCtrlClient(grpc::CreateChannel(svr_ip,
                grpc::InsecureChannelCredentials())));
    parser.reset();
}
ReplicateCtrl::~ReplicateCtrl(){
}

Status ReplicateCtrl::CreateReplication(ServerContext* context,
        const CreateReplicationReq* request,
        ReplicationCommonRes* response){
    const string& rep_id = request->rep_uuid();
    const string& local_vol = request->local_volume();
    const string& peer_vol = request->peer_volume();
    const string& operate_id = request->operate_id();
    const REP_ROLE& role = request->role();
    bool res = rep_ctrl_client_->create_replication(operate_id,
        rep_id,local_vol,peer_vol,role);
    if(res){
        response->set_ret(0);
    }
    else{
        LOG_ERROR << "create replication failed,vol_id=" << local_vol;
        response->set_ret(-1);
    }
    // TODO:check whether volume has old data
    return Status::OK;
}
Status ReplicateCtrl::EnableReplication(ServerContext* context,
        const EnableReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const string& operate_id = request->operate_id();
    const REP_ROLE& role = request->role();
    JournalMarker marker;
    // TODO: create snapshot
    bool res = rep_ctrl_client_->enable_replication(operate_id,
        local_vol,role,marker);
    if(res){
        response->set_ret(0);
    }
    else{
        LOG_ERROR << "enable replication failed,vol_id=" << local_vol;
        response->set_ret(-1);
    }
    return Status::OK;
}

Status ReplicateCtrl::DisableReplication(ServerContext* context,
        const DisableReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const string& operate_id = request->operate_id();
    const REP_ROLE& role = request->role();
    JournalMarker marker;
    // TODO: create snapshot
    bool res = rep_ctrl_client_->disable_replication(operate_id,
        local_vol,role,marker);
    if(res){
        response->set_ret(0);
    }
    else{
        LOG_ERROR << "disable replication failed,vol_id=" << local_vol;
        response->set_ret(-1);
    }
    return Status::OK;
}

Status ReplicateCtrl::FailoverReplication(ServerContext* context,
        const FailoverReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const string& operate_id = request->operate_id();
    const REP_ROLE& role = request->role();
    JournalMarker marker;
    // TODO:get sync ending postion
    bool res = rep_ctrl_client_->failover_replication(operate_id,
        local_vol,role,marker);
    if(res){
        response->set_ret(0);
    }
    else{
        LOG_ERROR << "failover replication failed,vol_id=" << local_vol;
        response->set_ret(-1);
    }
    return Status::OK;
}

Status ReplicateCtrl::ReverseReplication(ServerContext* context,
        const ReverseReplicationReq* request,
        ReplicationCommonRes* response){
    // TODO: implement
    response->set_ret(-1);
    return Status::OK;
}

Status ReplicateCtrl::DeleteReplication(ServerContext* context,
        const DeleteReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const string& operate_id = request->operate_id();
    const REP_ROLE& role = request->role();
    bool res = rep_ctrl_client_->delete_replication(operate_id,
        local_vol,role);
    if(res){
        response->set_ret(0);
    }
    else{
        LOG_ERROR << "delete replication failed,vol_id=" << local_vol;
        response->set_ret(-1);
    }
    return Status::OK;
}