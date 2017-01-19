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
using huawei::proto::inner::CreateVolumeMetaReq;
using huawei::proto::inner::CreateVolumeMetaRes;
using huawei::proto::inner::UpdateVolumeStatusReq;
using huawei::proto::inner::UpdateVolumeStatusRes;
using huawei::proto::inner::ReadVolumeMetaReq;
using huawei::proto::inner::ReadVolumeMetaRes;
using huawei::proto::inner::ListVolumeMetaReq;
using huawei::proto::inner::ListVolumeMetaRes;

class VolInnerCtrl:public huawei::proto::inner::VolumeInnerControl::Service{
    ::grpc::Status CreateVolumeMeta(ServerContext* context,
            const CreateVolumeMetaReq* request, CreateVolumeMetaRes* response);
    ::grpc::Status UpdateVolumeStatus(ServerContext* context,
            const UpdateVolumeStatusReq* request, UpdateVolumeStatusRes* response);
    ::grpc::Status ReadVolumeMeta(ServerContext* context,
            const ReadVolumeMetaReq* request, ReadVolumeMetaRes* response);
    ::grpc::Status ListVolumeMeta(ServerContext* context,
            const ListVolumeMetaRes* request, ListVolumeMetaRes* response);
    // add volume to gc when init
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
