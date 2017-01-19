/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    volume_inner_control.h
* Author: 
* Date:         2017/01/19
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef VOLUME_INNER_CONTROL_H_
#define VOLUME_INNER_CONTROL_H_
#include "rpc/volume_inner_control.grpc.pb.h"
#include "volume_meta_manager.h"
using ::grpc::ServerContext;
using huawei::proto::inner::CreateVolumeReq;
using huawei::proto::inner::CreateVolumeRes;
using huawei::proto::inner::UpdateVolumeStatusReq;
using huawei::proto::inner::UpdateVolumeStatusRes;
using huawei::proto::inner::GetVolumeReq;
using huawei::proto::inner::GetVolumeRes;
using huawei::proto::inner::ListVolumeReq;
using huawei::proto::inner::ListVolumeRes;

class VolInnerCtrl:public huawei::proto::inner::VolumeInnerControl::Service{
    ::grpc::Status CreateVolume(ServerContext* context,
            const CreateVolumeReq* request, CreateVolumeRes* response);
    ::grpc::Status UpdateVolumeStatus(ServerContext* context,
            const UpdateVolumeStatusReq* request, UpdateVolumeStatusRes* response);
    ::grpc::Status GetVolume(ServerContext* context,
            const GetVolumeReq* request, GetVolumeRes* response);
    ::grpc::Status ListVolume(ServerContext* context,
            const ListVolumeRes* request, ListVolumeRes* response);
    ::grpc::Status DeleteVolume(ServerContext* context,
        const GetVolumeReq* request, GetVolumeRes* response);
    void init();
public:
    VolInnerCtrl(std::shared_ptr<VolumeMetaManager> meta):
            meta_(meta){
        init();
    }
    ~VolInnerCtrl(){}
private:
    std::shared_ptr<VolumeMetaManager> meta_;
};
#endif
