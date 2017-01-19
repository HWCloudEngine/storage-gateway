/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    volume_inner_control.cc
* Author: 
* Date:         2017/01/19
* Version:      1.0
* Description:
* 
***********************************************/
#include "volume_inner_control.h"
#include "volume_meta_manager.h"
#include "log/log.h"
#include "gc_task.h"
#include "../rpc/common.pb.h"
using ::grpc::Status;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeInfo;
using huawei::proto::VOLUME_STATUS;
using huawei::proto::REPLAYER;
using huawei::proto::CONSUMER_BOTH;
using huawei::proto::sOk;
using huawei::proto::sInternalError;
using huawei::proto::sVolumeNotExist;
using huawei::proto::sVolumeMetaPersistError;
using huawei::proto::sVolumeAlreadyExist;
using huawei::proto::NO_SUCH_KEY;
void VolInnerCtrl::init(){
    std::list<VolumeMeta> vols;
    RESULT res = meta_->list_volume_meta(vols);
    DR_ASSERT(DRS_OK == res);
    for(VolumeMeta meta:vols){
        if(meta.info().rep_enable()){
            GCTask::instance().add_volume(meta.info().vol_id(),CONSUMER_BOTH);
        }
        else{
            GCTask::instance().add_volume(meta.info().vol_id(),REPLAYER);
        }
    }
}
Status VolInnerCtrl::CreateVolume(ServerContext* context,
        const CreateVolumeReq* request, CreateVolumeRes* response){
    const string& vol = request->vol_id();
    const string& path = request->path();
    const uint64_t size = request->size();
    const VOLUME_STATUS& status = request->status();
    VolumeMeta meta;
    VolumeInfo* info = meta.mutable_info();
    info->set_vol_id(vol);
    info->set_path(path);
    info->set_size(size);
    info->set_vol_status(status);
    info->set_rep_enable(false);
    RESULT res = meta_->create_volume(meta);
    if(DRS_OK == res){
        LOG_INFO << "create volume[" << vol << "] meta:\n"
            << "path=" << path << "\n"
            << "size=" << size << "\n"
            << "status=" << status;
        // add volume to GC
        GCTask::instance().add_volume(vol,REPLAYER);
        response->set_status(sOk);
    }
    else{
        response->set_status(sVolumeMetaPersistError);
        LOG_ERROR << "create volume[" << vol << "] failed!";
    }
    return Status::OK;
}
Status VolInnerCtrl::UpdateVolumeStatus(ServerContext* context,
        const UpdateVolumeStatusReq* request, UpdateVolumeStatusRes* response){
    const string& vol = request->vol_id();
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(vol,meta);
    if(DRS_OK != res){
        if(NO_SUCH_KEY == res)
            response->set_status(sVolumeNotExist);
        else
            response->set_status(sInternalError);
        return Status::OK;
    }
    LOG_INFO << "update volume[" << vol << "] status from "
        << meta.info().vol_status() << " to " << request->status();
    meta.mutable_info()->set_vol_status(request->status());
    res = meta_->update_volume_meta(meta);
    if(DRS_OK == res){
        response->set_status(sOk);
    }
    else{
        response->set_status(sVolumeMetaPersistError);
        LOG_ERROR << "update volume[" << vol << "] status failed!";
    }
    return Status::OK;
}
Status VolInnerCtrl::GetVolume(ServerContext* context,
        const GetVolumeReq* request, GetVolumeRes* response){
    const string& vol = request->vol_id();
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(vol,meta);
    if(DRS_OK == res){
        response->mutable_info()->CopyFrom(meta.info());
        response->set_status(sOk);
    }
    else if(NO_SUCH_KEY == res){
        response->set_status(sVolumeNotExist);
    }
    else{
        response->set_status(sInternalError);
    }
    return Status::OK;
}
Status VolInnerCtrl::ListVolume(ServerContext* context,
        const ListVolumeRes* request, ListVolumeRes* response){
    // TODO: implement
    response->set_status(sInternalError);
    return Status::OK;
}
Status VolInnerCtrl::DeleteVolume(ServerContext* context,
        const GetVolumeReq* request, GetVolumeRes* response){
    const string& vol = request->vol_id();
    RESULT res = meta_->delete_volume(vol);
    if(DRS_OK == res){
        response->set_status(sOk);
    }
    else if(NO_SUCH_KEY == res){
        response->set_status(sVolumeNotExist);
    }
    else{
        response->set_status(sInternalError);
    }
    return Status::OK;
}

