/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_inner_ctrl.c
* Author: 
* Date:         2017/01/03
* Version:      1.0
* Description: provides control api to sg client
* 
************************************************/
#include "rep_inner_ctrl.h"
#include "log/log.h"
#include "gc_task.h"
#include <time.h> // time,time_t
#include <chrono>
#include <functional>
using grpc::Status;
using huawei::proto::OperationRecord;
using huawei::proto::sOk;
using huawei::proto::sInternalError;
using huawei::proto::sVolumeNotExist;
using huawei::proto::sReplicationNotExist;
using huawei::proto::sInvalidOperation;
using huawei::proto::sReplicationMetaPersistError;
void RepInnerCtrl::init(){

}
// create replication with empty volume(no old data)
Status RepInnerCtrl::CreateReplication(ServerContext* context,
        const CreateReplicationInnerReq* request,
        ReplicationInnerCommonRes* response){
    const string& local_vol = request->local_volume();
    const string& uuid = request->rep_uuid();
    const huawei::proto::REP_ROLE& role = request->role();
    const string& op_id = request->operate_id();
    LOG_INFO << "create replication:\n"
        << "local volume:" << local_vol << "\n"
        << "replication uuid:" << uuid << "\n"
        << "role:" << role << "\n"
        << "peer volume count:" << request->peer_volumes_size() << "\n";
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(local_vol,meta);
//    if(NO_SUCH_KEY == res){
//        LOG_ERROR << "create replication failed,volume not found:"
//            << request.vol_id();
//        response->set_status(sVolumeNotExist);
//        return grpc::Status::OK;
//    }
    meta.mutable_info()->set_rep_uuid(uuid);
    meta.mutable_info()->set_vol_id(local_vol);
    for(string vol:request->peer_volumes()){
        meta.mutable_info()->add_peer_volumes(vol);
        LOG_INFO << "\t peer volume: " << vol;
    }
    meta.mutable_info()->set_role(role);
    meta.mutable_info()->set_rep_status(REP_CREATING);
    meta.mutable_info()->set_rep_enable(true);
    meta.clear_records();
    OperationRecord* record = meta.add_records();
    record->set_operate_id(op_id);
    record->set_type(REPLICATION_CREATE);
    record->set_time(time(nullptr));
    res = meta_->create_volume(meta);// TODO: replace with update_volume_meta api
    if(DRS_OK != res){
        LOG_ERROR << "create replication failed!";
        response->set_status(sReplicationMetaPersistError);
        return grpc::Status::OK;
    }
    // replicator has the liability to update its markers
    if(role == REP_PRIMARY){
        // add volume to gc
        GCTask::instance().add_volume(local_vol,REPLICATOR);
        // add volume to replicate
        replicate_.add_volume(local_vol);
    }
    response->set_status(sOk);
    return grpc::Status::OK;
}
Status RepInnerCtrl::EnableReplication(ServerContext* context,
        const EnableReplicationInnerReq* request,
        ReplicationInnerCommonRes* response){
    const string& local_vol = request->vol_id();
    const huawei::proto::REP_ROLE& role = request->role();
    const string& op_id = request->operate_id();
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(local_vol,meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_ENABLE)){
        LOG_ERROR << "enable replication " << local_vol
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_status(sInvalidOperation);
        return grpc::Status::OK;
    }
    // update replication meta
    meta.mutable_info()->set_rep_status(REP_ENABLING);
    huawei::proto::OperationRecord* op = meta.add_records();
    op->set_operate_id(op_id);
    op->set_type(REPLICATION_ENABLE);
    op->set_time(time(nullptr));
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(local_vol);
    response->set_status(sOk);
    return Status::OK;
}
Status RepInnerCtrl::DisableReplication(ServerContext* context,
        const DisableReplicationInnerReq* request,
        ReplicationInnerCommonRes* response){
    const string& local_vol = request->vol_id();
    const huawei::proto::REP_ROLE& role = request->role();
    const string& op_id = request->operate_id();
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(local_vol,meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_DISABLE)){
        LOG_ERROR << "disable replication " << local_vol
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_status(sInvalidOperation);
        return grpc::Status::OK;
    }

    // update replication meta
    meta.mutable_info()->set_rep_status(REP_DISABLING);
    huawei::proto::OperationRecord* op = meta.add_records();
    op->set_operate_id(op_id);
    op->set_type(REPLICATION_DISABLE);
    op->set_time(time(nullptr));
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(local_vol);
    response->set_status(sOk);
    return Status::OK;
}
Status RepInnerCtrl::FailoverReplication(ServerContext* context,
        const FailoverReplicationInnerReq* request,
        ReplicationInnerCommonRes* response){
    const string& local_vol = request->vol_id();
    const huawei::proto::REP_ROLE& role = request->role();
    const string& op_id = request->operate_id();
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(local_vol,meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_FAILOVER)){
        LOG_ERROR << "failover replication " << local_vol
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_status(sInvalidOperation);
        return grpc::Status::OK;
    }

    // update replication meta
    meta.mutable_info()->set_rep_status(REP_FAILING_OVER);
    huawei::proto::OperationRecord* op = meta.add_records();
    op->set_operate_id(op_id);
    op->set_type(REPLICATION_FAILOVER);
    op->set_time(time(nullptr));
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(local_vol);
    response->set_status(sOk);
    return Status::OK;
}
Status RepInnerCtrl::ReverseReplication(ClientContext* context,
        const ReverseReplicationInnerReq* request,
        ReplicationInnerCommonRes* response){
    // TODO:
    Status status(grpc::INTERNAL,"not implement.");
    return status;
}

Status RepInnerCtrl::DeleteReplication(ServerContext* context,
        const DeleteReplicationInnerReq* request,
        ReplicationInnerCommonRes* response){
    const string& local_vol = request->vol_id();
    const huawei::proto::REP_ROLE& role = request->role();
    const string& op_id = request->operate_id();
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(local_vol,meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_DELETE)){
        LOG_ERROR << "delete replication " << local_vol
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_status(sInvalidOperation);
        return grpc::Status::OK;
    }
    if(role == REP_PRIMARY){
        replicate_.remove_volume(meta.info().vol_id());
    }
    else{
    // TODO:recycle journals???
    }
    meta.mutable_info()->set_rep_enable(false);
    meta.mutable_info()->set_rep_status(REP_DELETING);
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(local_vol);
    response->set_status(sOk);
    return Status::OK;
}

Status RepInnerCtrl::ReportCheckpoint(ServerContext* context,
            const ReportCheckpointReq* request,
            ReportCheckpointRes* response){
    // TODO:
    Status status(grpc::INTERNAL,"not implement.");
    return status;
}

void RepInnerCtrl::notify_rep_state_changed(const string& vol){
}
bool RepInnerCtrl::validate_replicate_operation(
        const REP_STATUS& status,
        const REPLICATION_OPERATION& op){
    switch(op){
    // TODO: validate the operation from controller?
        case REPLICATION_ENABLE:
            if(status == REP_DISABLED)
                return true;
            else
                return false;
        case REPLICATION_DISABLE:
            if(status == REP_ENABLED)
                return true;
            else
                return false;
        case REPLICATION_FAILOVER:
            if(status == REP_ENABLED)
                return true;
            else
                return false;
        case REPLICATION_QUERY:
        case REPLICATION_LIST:
        case REPLICATION_DELETE:
        default:
            return true;
    }
    return false;
}

