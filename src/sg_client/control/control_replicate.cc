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
#include <boost/uuid/uuid.hpp> // uuid class
#include <boost/uuid/uuid_io.hpp> // streaming operators
#include "control_replicate.h"
#include "common/config_parser.h"
#include "../../log/log.h"
using std::string;
using huawei::proto::sOk;
using huawei::proto::sInternalError;
using huawei::proto::sVolumeNotExist;
using huawei::proto::sVolumeMetaPersistError;
using huawei::proto::sVolumeAlreadyExist;
ReplicateCtrl::ReplicateCtrl(std::map<string, std::shared_ptr<Volume>>& volumes):
        volumes_(volumes){
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    string default_ip("127.0.0.1");
    string svr_ip = parser->get_default("meta_server.ip",default_ip);
    int svr_port = parser->get_default("meta_server.port",50051);
    svr_ip += ":" + std::to_string(svr_port);
    rep_ctrl_client_.reset(new RepInnerCtrlClient(grpc::CreateChannel(svr_ip,
                grpc::InsecureChannelCredentials())));
    parser.reset();
}
ReplicateCtrl::~ReplicateCtrl(){
}

std::shared_ptr<ReplicateProxy> ReplicateCtrl::get_replicate_proxy(
                const string& vol_name){
    auto it = volumes_.find(vol_name);
    if(it != volumes_.end()){
        return it->second->get_replicate_proxy();
    }
    LOG_ERROR << "get_replicate_proxy vid:" << vol_name << "failed";
    return nullptr;
}

Status ReplicateCtrl::CreateReplication(ServerContext* context,
        const CreateReplicationReq* request,
        ReplicationCommonRes* response){
    const string& rep_id = request->rep_uuid();
    const string& local_vol = request->local_volume();
    const RepRole& role = request->role();
    string operate_id = boost::uuids::to_string(uuid_generator_());
    LOG_INFO << "create replication:\n"
        << "local volume:" << local_vol << "\n"
        << "replication uuid:" << rep_id << "\n"
        << "role:" << role << "\n"
        << "peer volume :" << "\n";
    std::list<string> peer_vols;
    for(int i=0;i<request->peer_volumes_size();i++){
        peer_vols.push_back(request->peer_volumes(i));
        LOG_INFO << "\t" << request->peer_volumes(i);
    }


    StatusCode res = rep_ctrl_client_->create_replication(operate_id,
        rep_id,local_vol,peer_vols,role);
    if(res){
        LOG_ERROR << "create replication failed,vol_id=" << local_vol;
    }
    response->set_status(res);
    // TODO:check whether volume has old data
    return Status::OK;
}

Status ReplicateCtrl::EnableReplication(ServerContext* context,
        const EnableReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const RepRole& role = request->role();
    StatusCode res;

    string operate_id = boost::uuids::to_string(uuid_generator_());
    LOG_INFO << "enable replication:\n"
        << "local volume:" << local_vol << "\n"
        << "operation uuid:" << operate_id << "\n"
        << "role:" << role;

    JournalMarker marker;
    if(RepRole::REP_PRIMARY == role){
    // 1. create snapshot for replication
        std::shared_ptr<ReplicateProxy> rep_proxy = get_replicate_proxy(local_vol);
        if(rep_proxy == nullptr){
            LOG_ERROR << "replicate proxy not found for volume: " << local_vol;
            response->set_status(StatusCode::sInternalError);
            return Status::OK;
        }

        string snap_name = rep_proxy->operate_uuid_to_snap_name(operate_id);
        res = rep_proxy->create_snapshot(snap_name,marker);
        if(!res){
            LOG_ERROR << "create snapshot[" << operate_id << "] for volume["
                << local_vol << "] enable failed!";
            response->set_status(res);
            return Status::OK;
        }
    }
    // 2. update replication meta
    res = rep_ctrl_client_->enable_replication(operate_id,
        local_vol,role,marker);
    if(res){
        LOG_ERROR << "enable replication failed,vol_id=" << local_vol;
    }
    response->set_status(res);
    return Status::OK;
}

Status ReplicateCtrl::DisableReplication(ServerContext* context,
        const DisableReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const RepRole& role = request->role();
    StatusCode res;

    string operate_id = boost::uuids::to_string(uuid_generator_());
    LOG_INFO << "disable replication:\n"
        << "local volume:" << local_vol << "\n"
        << "operation uuid:" << operate_id << "\n"
        << "role:" << role;

    JournalMarker marker;
    if(RepRole::REP_PRIMARY == role){
    // 1. create snapshot for replication
        std::shared_ptr<ReplicateProxy> rep_proxy = get_replicate_proxy(local_vol);
        if(rep_proxy == nullptr){
            LOG_ERROR << "replicate proxy not found for volume: " << local_vol;
            response->set_status(StatusCode::sInternalError);
            return Status::OK;
        }

        string snap_name = rep_proxy->operate_uuid_to_snap_name(operate_id);
        res = rep_proxy->create_snapshot(snap_name,marker);
        if(!res){
            LOG_ERROR << "create snapshot[" << operate_id << "] for volume["
                << local_vol << "] enable failed!";
            response->set_status(res);
            return Status::OK;
        }
    }
    // 2. update replication meta
    res = rep_ctrl_client_->disable_replication(operate_id,
        local_vol,role,marker);
    if(res){
        LOG_ERROR << "disable replication failed,vol_id=" << local_vol;
    }
    response->set_status(res);
    return Status::OK;
}

Status ReplicateCtrl::FailoverReplication(ServerContext* context,
        const FailoverReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const RepRole& role = request->role();
    StatusCode res;

    string operate_id = boost::uuids::to_string(uuid_generator_());
    LOG_INFO << "failover replication:\n"
        << "local volume:" << local_vol << "\n"
        << "operation uuid:" << operate_id << "\n"
        << "role:" << role;

    JournalMarker marker;
    if(RepRole::REP_PRIMARY == role){
    // 1. create snapshot for replication
        std::shared_ptr<ReplicateProxy> rep_proxy = get_replicate_proxy(local_vol);
        if(rep_proxy == nullptr){
            LOG_ERROR << "replicate proxy not found for volume: " << local_vol;
            response->set_status(StatusCode::sInternalError);
            return Status::OK;
        }

        string snap_name = rep_proxy->operate_uuid_to_snap_name(operate_id);
        res = rep_proxy->create_snapshot(snap_name,marker);
        if(!res){
            LOG_ERROR << "create snapshot[" << operate_id << "] for volume["
                << local_vol << "] enable failed!";
            response->set_status(res);
            return Status::OK;
        }
    }

    // 2. update replicate meta
    res = rep_ctrl_client_->failover_replication(operate_id,
        local_vol,role,marker);
    if(res){
        LOG_ERROR << "failover replication failed,vol_id=" << local_vol;
    }
    response->set_status(res);
    return Status::OK;
}

Status ReplicateCtrl::ReverseReplication(ServerContext* context,
        const ReverseReplicationReq* request,
        ReplicationCommonRes* response){
    // TODO: implement
    response->set_status(sInternalError);
    return Status::OK;
}

Status ReplicateCtrl::DeleteReplication(ServerContext* context,
        const DeleteReplicationReq* request,
        ReplicationCommonRes* response){
    const string& local_vol = request->vol_id();
    const RepRole& role = request->role();

    string operate_id = boost::uuids::to_string(uuid_generator_());
    LOG_INFO << "delete replication:\n"
        << "local volume:" << local_vol << "\n"
        << "operation uuid:" << operate_id << "\n"
        << "role:" << role;

    StatusCode res = rep_ctrl_client_->delete_replication(operate_id,
        local_vol,role);
    if(res){
        LOG_ERROR << "delete replication failed,vol_id=" << local_vol;
    }
    response->set_status(res);
    return Status::OK;
}
