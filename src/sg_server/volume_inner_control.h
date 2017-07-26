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
using huawei::proto::VolumeInfo;
using huawei::proto::StatusCode;

class VolInnerCtrl: public huawei::proto::inner::VolumeInnerControl::Service {
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

    StatusCode get_volume(const std::string& vol, VolumeInfo& vol_info);

    void init(std::shared_ptr<VolumeMetaManager> v_meta,
            std::shared_ptr<JournalMetaManager> j_meta);

    static VolInnerCtrl& instance();

    VolInnerCtrl(VolInnerCtrl&) = delete;

    VolInnerCtrl& operator=(VolInnerCtrl const&) = delete;

private:
    VolInnerCtrl();
    ~VolInnerCtrl();

    std::shared_ptr<VolumeMetaManager> vmeta_;
    std::shared_ptr<JournalMetaManager> jmeta_;
};

#endif
