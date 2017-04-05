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
#include "log/log.h"
using std::string;
using huawei::proto::sOk;
using huawei::proto::sInternalError;
using huawei::proto::sVolumeNotExist;
using huawei::proto::sVolumeMetaPersistError;
using huawei::proto::sVolumeAlreadyExist;

ReplicateCtrl::ReplicateCtrl(const Configure& conf, std::map<string, std::shared_ptr<Volume>>& volumes):
        conf_(conf), volumes_(volumes){

   rep_ctrl_client_.reset(new RepInnerCtrlClient(grpc::CreateChannel(conf_.sg_server_addr(),
                grpc::InsecureChannelCredentials())));
}

ReplicateCtrl::~ReplicateCtrl(){
}

std::shared_ptr<ReplicateProxy> ReplicateCtrl::get_replicate_proxy(
                const string& vol_name){
    auto it = volumes_.find(vol_name);
    if(it != volumes_.end()){
        return it->second->get_replicate_proxy();
    }
    LOG_ERROR << "get_replicate_proxy vol-id:" << vol_name << " failed";
    return nullptr;
}

std::shared_ptr<JournalWriter> ReplicateCtrl::get_journal_writer(
        const string& vol_name){
    auto it = volumes_.find(vol_name);
    if(it != volumes_.end()){
        return it->second->get_writer();
    }
    LOG_ERROR << "get_journal_writer vol-id:" << vol_name << " failed";
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

    // 1. hold producer marker
    //  for enable, here hold producer marker to confirm that the replicator
    //  should not replicate any data written before the snapshot synced
    std::shared_ptr<JournalWriter> writer = get_journal_writer(local_vol);
    if(writer == nullptr){
        LOG_ERROR << "hold producer marker failed, vol[" << local_vol
            << "] not found.";
        response->set_status(StatusCode::sInternalError);
        return Status::OK;
    }
    writer->hold_producer_marker();

    JournalMarker marker;
    if(RepRole::REP_PRIMARY == role){
    // 2. create snapshot for replication
        std::shared_ptr<ReplicateProxy> rep_proxy = get_replicate_proxy(local_vol);
        if(rep_proxy == nullptr){
            LOG_ERROR << "replicate proxy not found for volume: " << local_vol;
            response->set_status(StatusCode::sInternalError);
            return Status::OK;
        }

        string snap_name = rep_proxy->operate_uuid_to_snap_name(operate_id);
        res = rep_proxy->create_snapshot(snap_name,marker);
        if(res){
            LOG_ERROR << "create snapshot[" << operate_id << "] for volume["
                << local_vol << "] enable failed!";
        }
    }
    // 3. update replication meta if snapshot created
    if(!res){
        res = rep_ctrl_client_->enable_replication(operate_id,
            local_vol,role,marker);
        if(res){
            LOG_ERROR << "enable replication failed,vol_id=" << local_vol;
        }
    }
    //4. unhold producer marker
    writer->unhold_producer_marker();
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

    // 1. hold producer marker
    // for disable, here hold producer marker to confirm that the replicator
    //  should not replicate any data written after the disable snapshot entry
    std::shared_ptr<JournalWriter> writer = get_journal_writer(local_vol);
    if(writer == nullptr){
        LOG_ERROR << "hold producer marker failed, vol[" << local_vol
            << "] not found.";
        response->set_status(StatusCode::sInternalError);
        return Status::OK;
    }
    writer->hold_producer_marker();

    JournalMarker marker;
    if(RepRole::REP_PRIMARY == role){
    // 2. create snapshot for replication
        std::shared_ptr<ReplicateProxy> rep_proxy = get_replicate_proxy(local_vol);
        if(rep_proxy == nullptr){
            LOG_ERROR << "replicate proxy not found for volume: " << local_vol;
            res = StatusCode::sInternalError;
        }

        string snap_name = rep_proxy->operate_uuid_to_snap_name(operate_id);
        res = rep_proxy->create_snapshot(snap_name,marker);
        if(res){
            LOG_ERROR << "create snapshot[" << operate_id << "] for volume["
                << local_vol << "] enable failed!";
        }
    }
    // 3. update replication meta if snapshot created
    if(!res){
        res = rep_ctrl_client_->disable_replication(operate_id,
            local_vol,role,marker);
        if(res){
            LOG_ERROR << "disable replication failed,vol_id=" << local_vol;
        }
    }

    // 4. unhold producer marker
    writer->unhold_producer_marker();

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

    // 1. hold producer marker
    std::shared_ptr<JournalWriter> writer = get_journal_writer(local_vol);
    if(writer == nullptr){
        LOG_ERROR << "hold producer marker failed, vol[" << local_vol
            << "] not found.";
        response->set_status(StatusCode::sInternalError);
        return Status::OK;
    }
    writer->hold_producer_marker();

    JournalMarker marker;
    if(RepRole::REP_PRIMARY == role){
    // 2. create snapshot for replication
        std::shared_ptr<ReplicateProxy> rep_proxy = get_replicate_proxy(local_vol);
        if(rep_proxy == nullptr){
            LOG_ERROR << "replicate proxy not found for volume: " << local_vol;
            res = StatusCode::sInternalError;
        }

        string snap_name = rep_proxy->operate_uuid_to_snap_name(operate_id);
        res = rep_proxy->create_snapshot(snap_name,marker);
        if(res){
            LOG_ERROR << "create snapshot[" << operate_id << "] for volume["
                << local_vol << "] enable failed!";
        }
    }

    // 3. update replicate meta if snapshot created
    if(!res){
        res = rep_ctrl_client_->failover_replication(operate_id,
            local_vol,role,marker);
        if(res){
            LOG_ERROR << "failover replication failed,vol_id=" << local_vol;
        }
    }

    // 4. unhold producer marker
    writer->unhold_producer_marker();

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
