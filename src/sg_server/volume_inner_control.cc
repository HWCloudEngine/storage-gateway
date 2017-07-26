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
#include "replayer_context.h"
#include "../rpc/common.pb.h"
using ::grpc::Status;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeInfo;
using huawei::proto::VolumeStatus;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATOR;
using huawei::proto::sOk;
using huawei::proto::sInternalError;
using huawei::proto::sVolumeNotExist;
using huawei::proto::sVolumeMetaPersistError;
using huawei::proto::sVolumeAlreadyExist;
using huawei::proto::NO_SUCH_KEY;

VolInnerCtrl::VolInnerCtrl() {
}

VolInnerCtrl::~VolInnerCtrl() {
}

void VolInnerCtrl::init(std::shared_ptr<VolumeMetaManager> v_meta,
                        std::shared_ptr<JournalMetaManager> j_meta) {
    vmeta_ = v_meta;
    jmeta_ = j_meta;
}

VolInnerCtrl& VolInnerCtrl::instance() {
    static VolInnerCtrl vol_ctrl;
    return vol_ctrl;
}

Status VolInnerCtrl::CreateVolume(ServerContext* context,
        const CreateVolumeReq* request, CreateVolumeRes* response){
    const string& vol = request->vol_id();
    const string& path = request->path();
    const uint64_t size = request->size();
    const VolumeStatus& status = request->status();
    VolumeMeta meta;
    RESULT res = vmeta_->read_volume_meta(vol,meta);
    if(DRS_OK == res){
        LOG_ERROR << "volume[" << vol << "] already exsit!";
        response->set_status(sVolumeAlreadyExist);
        return Status::OK;
    }
    VolumeInfo* info = meta.mutable_info();
    info->set_vol_id(vol);
    info->set_path(path);
    info->set_size(size);
    info->set_vol_status(status);
    info->set_rep_enable(false);
    res = vmeta_->create_volume(meta);
    if(DRS_OK == res){
        LOG_INFO << "create volume[" << vol << "] meta:\n"
            << "path=" << path << "\n"
            << "size=" << size << "\n"
            << "status=" << status;
        // add volume to GC
        GCTask::instance().add_volume(vol);
        ReplayerContext* c = new ReplayerContext(vol,jmeta_);
        GCTask::instance().register_consumer(vol, c);
        response->set_status(sOk);
    }
    else{
        response->set_status(sVolumeMetaPersistError);
        LOG_ERROR << "create volume[" << vol << "] failed!";
    }
    return Status::OK;
}

Status VolInnerCtrl::UpdateVolume(ServerContext* context,
        const UpdateVolumeReq* request, UpdateVolumeRes* response){
    const string& vol = request->vol_id();
    const VolumeStatus& s = request->status();
    const string& path = request->path();
    const string& attached_host = request->attached_host();
    VolumeMeta meta;
    RESULT res = vmeta_->read_volume_meta(vol,meta);
    if(DRS_OK != res){
        if(NO_SUCH_KEY == res)
            response->set_status(sVolumeNotExist);
        else
            response->set_status(sInternalError);
        return Status::OK;
    }
    if(s != VolumeStatus::VOL_UNKNOWN){
        LOG_INFO << "update volume[" << vol << "] status from "
            << meta.info().vol_status() << " to " << s;
        meta.mutable_info()->set_vol_status(s);
    }
    if(!path.empty()){
        LOG_INFO << "update volume[" << vol << "] path from "
            << meta.info().path() << " to " << path;
        meta.mutable_info()->set_path(path);
    }
    if(!attached_host.empty()){
        LOG_INFO << "update volume[" << vol << "] attached_host from "
            << meta.info().attached_host() << " to " << attached_host;
        meta.mutable_info()->set_attached_host(attached_host);
    }
    res = vmeta_->update_volume_meta(meta);
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
    response->set_status(get_volume(vol,*(response->mutable_info())));
    return Status::OK;
}

Status VolInnerCtrl::ListVolume(ServerContext* context,
        const ListVolumeReq* request, ListVolumeRes* response){
    std::list<VolumeMeta> list;
    RESULT res = vmeta_->list_volume_meta(list);
    if(DRS_OK == res){
        response->set_status(sOk);
        LOG_DEBUG << "list volume, volume count:" << list.size();
        for(auto meta:list){
            LOG_DEBUG << " volume:" << meta.info().vol_id();
            LOG_DEBUG << "  vol status:" << meta.info().vol_status();
            LOG_DEBUG << "  replicate status:" << meta.info().rep_status();
            response->add_volumes()->CopyFrom(meta.info());
        }
    }
    else{
        LOG_ERROR << "list volume meta failed!";
        response->set_status(sInternalError);
    }
    return Status::OK;
}
Status VolInnerCtrl::DeleteVolume(ServerContext* context,
        const DeleteVolumeReq* request, DeleteVolumeRes* response){
    const string& vol = request->vol_id();

    // TODO: confirm that replication is deleted
    RESULT res = vmeta_->delete_volume(vol);
    if(DRS_OK == res){
        LOG_INFO << "delete volume[" << vol << "], done!";
        GCTask::instance().unregister_consumer(vol,REPLAYER);
        GCTask::instance().unregister_consumer(vol,REPLICATOR);
        GCTask::instance().remove_volume(vol);
        response->set_status(sOk);
    }
    else if(NO_SUCH_KEY == res){
        LOG_WARN << "delete volume[" << vol << "], not found!";
        response->set_status(sVolumeNotExist);
    }
    else{
        LOG_ERROR << "delete volume[" << vol << "] failed!";
        response->set_status(sInternalError);
    }
    return Status::OK;
}

StatusCode VolInnerCtrl::get_volume(const std::string& vol,
                                        VolumeInfo& vol_info){
    VolumeMeta meta;
    RESULT res = vmeta_->read_volume_meta(vol,meta);
    if(DRS_OK == res){
        vol_info.CopyFrom(meta.info());
        return sOk;
    }
    else if(NO_SUCH_KEY == res){
        return sVolumeNotExist;
    }
    else{
        LOG_ERROR << "get volume[" << vol << "] failed!";
        return sInternalError;
    }
}
