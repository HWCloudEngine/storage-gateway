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
#include "common/observe.h"
#include "rpc/volume_inner_control.grpc.pb.h"
#include "volume_meta_manager.h"
#include "journal_meta_manager.h"
using ::grpc::ServerContext;
using huawei::proto::inner::CreateVolumeReq;
using huawei::proto::inner::CreateVolumeRes;
using huawei::proto::inner::UpdateVolumeReq;
using huawei::proto::inner::UpdateVolumeRes;
using huawei::proto::inner::GetVolumeReq;
using huawei::proto::inner::GetVolumeRes;
using huawei::proto::inner::ListVolumeReq;
using huawei::proto::inner::ListVolumeRes;
using huawei::proto::inner::DeleteVolumeReq;
using huawei::proto::inner::DeleteVolumeRes;

enum VolumeEvent {
    UNKNOWN        = 0,
    CREATE_VOLUME  = 1,
    UPDATE_VOLUME  = 2,
    DELETE_VOLUME  = 3,
};

class VolInnerCtrl: public huawei::proto::inner::VolumeInnerControl::Service, public Observer {
public:
    ::grpc::Status CreateVolume(ServerContext* context,
            const CreateVolumeReq* request, CreateVolumeRes* response);
    ::grpc::Status UpdateVolume(ServerContext* context,
            const UpdateVolumeReq* request, UpdateVolumeRes* response);
    ::grpc::Status GetVolume(ServerContext* context,
            const GetVolumeReq* request, GetVolumeRes* response);
    ::grpc::Status ListVolume(ServerContext* context,
            const ListVolumeReq* request, ListVolumeRes* response);
    ::grpc::Status DeleteVolume(ServerContext* context,
        const DeleteVolumeReq* request, DeleteVolumeRes* response);
    void init();
    
    /*volume status update notify all subscribers*/
    void notify(int event, void* args) override;

public:
    VolInnerCtrl(std::shared_ptr<VolumeMetaManager> v_meta,
            std::shared_ptr<JournalMetaManager> j_meta):
            vmeta_(v_meta),
            jmeta_(j_meta){
        init();
    }
    ~VolInnerCtrl(){}
private:
    std::shared_ptr<VolumeMetaManager> vmeta_;
    std::shared_ptr<JournalMetaManager> jmeta_;
};

extern VolInnerCtrl* g_vol_ctrl;

#endif
