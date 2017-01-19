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

void RepInnerCtrl::init(){

}
// create replication with empty volume(no old data)
Status RepInnerCtrl::CreateReplication(ServerContext* context,
        const CreateReplicationInnerReq& request,
        ReplicationInnerCommonRes* response){
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(request.local_volume(),meta);
//    if(NO_SUCH_KEY == res){
//        LOG_ERROR << "create replication failed,volume not found:"
//            << request.vol_id();
//        response->set_ret(-1);
//        return grpc::Status::OK;
//    }
    meta.mutable_info()->set_rep_uuid(request.rep_uuid());
    meta.mutable_info()->set_vol_id(request.local_volume());
    meta.mutable_info()->set_peer_volume(request.peer_volume());
    meta.mutable_info()->set_role(request.role());
    meta.mutable_info()->set_rep_status(REP_CREATING);
    meta.mutable_info()->set_rep_enable(true);
    meta.mutable_records(0)->set_operate_id(request.operate_id());
    meta.mutable_records(0)->set_type(REPLICATION_CREATE);
    meta.mutable_records(0)->set_time(time(nullptr));
    res = meta_->create_volume(meta);// TODO: replace with update_volume_meta api
    if(DRS_OK != res){
        LOG_ERROR << "create replication failed!";
        response->set_ret(-1);
        return grpc::Status::OK;
    }
    // replicator has the liability to update its markers
    if(request.role() == REP_PRIMARY){
        // add volume to gc
        GCTask::instance().add_volume(request.local_volume(),REPLICATOR);
        // add volume to replicate
        replicate_.add_volume(request.local_volume());
    }
    return grpc::Status::OK;
}
Status RepInnerCtrl::EnableReplication(ServerContext* context,
        const EnableReplicationInnerReq& request,
        ReplicationInnerCommonRes* response){
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(request.vol_id(),meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_ENABLE)){
        LOG_ERROR << "enable replication " << request.vol_id()
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_ret(-1);
        return grpc::Status::OK;
    }
    // update replication meta
    meta.mutable_info()->set_rep_status(REP_ENABLING);
    huawei::proto::OperationRecord* op = meta.add_records();
    op->set_operate_id(request.operate_id());
    op->set_type(REPLICATION_ENABLE);
    op->set_time(time(nullptr));
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(request.vol_id());
    response->set_ret(0);
    return Status::OK;
}
Status RepInnerCtrl::DisableReplication(ServerContext* context,
        const DisableReplicationInnerReq& request,
        ReplicationInnerCommonRes* response){
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(request.vol_id(),meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_DISABLE)){
        LOG_ERROR << "disable replication " << request.vol_id()
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_ret(-1);
        return grpc::Status::OK;
    }

    // update replication meta
    meta.mutable_info()->set_rep_status(REP_DISABLING);
    huawei::proto::OperationRecord* op = meta.add_records();
    op->set_operate_id(request.operate_id());
    op->set_type(REPLICATION_DISABLE);
    op->set_time(time(nullptr));
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(request.vol_id());
    response->set_ret(0);
    return Status::OK;
}
Status RepInnerCtrl::FailoverReplication(ServerContext* context,
        const FailoverReplicationInnerReq& request,
        ReplicationInnerCommonRes* response){
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(request.vol_id(),meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_FAILOVER)){
        LOG_ERROR << "failover replication " << request.vol_id()
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_ret(-1);
        return grpc::Status::OK;
    }

    // update replication meta
    meta.mutable_info()->set_rep_status(REP_FAILING_OVER);
    huawei::proto::OperationRecord* op = meta.add_records();
    op->set_operate_id(request.operate_id());
    op->set_type(REPLICATION_FAILOVER);
    op->set_time(time(nullptr));
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(request.vol_id());
    response->set_ret(0);
    return Status::OK;
}
Status RepInnerCtrl::ReverseReplication(ClientContext* context,
        const ReverseReplicationInnerReq& request,
        ReplicationInnerCommonRes* response){
    // TODO:
    Status status(grpc::INTERNAL,"not implement.");
    return status;
}

Status RepInnerCtrl::DeleteReplication(ServerContext* context,
        const DeleteReplicationInnerReq& request,
        ReplicationInnerCommonRes* response){
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(request.vol_id(),meta);
    DR_ASSERT(DRS_OK == res);
    if(!validate_replicate_operation(meta.info().rep_status(),REPLICATION_DELETE)){
        LOG_ERROR << "delete replication " << request.vol_id()
            << " failed:not allowed at current state,"
            << meta.info().rep_status();
        response->set_ret(-1);
        return grpc::Status::OK;
    }
    if(request.role() == REP_PRIMARY){
        replicate_.remove_volume(meta.info().vol_id());
    }
    else{
    // TODO:recycle journals???
    }
    meta.mutable_info()->set_rep_enable(false);
    res = meta_->update_volume_meta(meta);
    DR_ASSERT(DRS_OK == res);
    notify_rep_state_changed(request.vol_id());
    response->set_ret(0);
    return Status::OK;
}

Status ReportReplicationCP(ServerContext* context,
            const ReportReplicationCPReq& request,
            ReportReplicationCPRes* response){
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

