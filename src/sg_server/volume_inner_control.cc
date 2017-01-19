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
using ::grpc::Status;
Status VolInnerCtrl::CreateVolumeMeta(ServerContext* context,
        const CreateVolumeMetaReq* request, CreateVolumeMetaRes* response){
    // TODO: implement
    response->set_ret(-1);
    return Status::OK;
}
Status VolInnerCtrl::UpdateVolumeStatus(ServerContext* context,
        const UpdateVolumeStatusReq* request, UpdateVolumeStatusRes* response){
    // TODO: implement
    response->set_ret(-1);
    return Status::OK;
}
Status VolInnerCtrl::ReadVolumeMeta(ServerContext* context,
        const ReadVolumeMetaReq* request, ReadVolumeMetaRes* response){
    // TODO: implement
    response->set_ret(-1);
    return Status::OK;
}
Status VolInnerCtrl::ListVolumeMeta(ServerContext* context,
        const ListVolumeMetaRes* request, ListVolumeMetaRes* response){
    // TODO: implement
    response->set_ret(-1);
    return Status::OK;
}

