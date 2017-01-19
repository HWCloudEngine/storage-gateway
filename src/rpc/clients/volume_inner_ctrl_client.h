/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    volume_inner_ctrl_client.h
* Author: 
* Date:         2017/01/19
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef VOLUME_INNER_CTRL_CLIENT_H_
#define VOLUME_INNER_CTRL_CLIENT_H_
#include <list>
#include <string>
#include "../volume_inner_control.grpc.pb.h"
using grpc::ClientContext;
using huawei::proto::inner::CreateVolumeMetaReq;
using huawei::proto::inner::CreateVolumeMetaRes;
using huawei::proto::inner::UpdateVolumeStatusReq;
using huawei::proto::inner::UpdateVolumeStatusRes;
using huawei::proto::inner::ReadVolumeMetaReq;
using huawei::proto::inner::ReadVolumeMetaRes;
using huawei::proto::inner::ListVolumeMetaReq;
using huawei::proto::inner::ListVolumeMetaRes;
using huawei::proto::VolumeInfo;
using huawei::proto::VolumeMeta;
using huawei::proto::VOLUME_STATUS;
class VolInnerCtrlClient{
public:
    VolInnerCtrlClient(std::shared_ptr<grpc::Channel> channel):
        stub_(huawei::proto::inner::VolumeInnerControl::NewStub(channel)){
    }
    ~VolInnerCtrlClient(){}
    int create_volume(const std::string& vol,const std::string& path,
                const uint64_t& size, const VOLUME_STATUS& s){
        ClientContext context;
        CreateVolumeMetaReq request;
        CreateVolumeMetaRes response;
        request.set_vol_id(vol);
        request.set_path(path);
        request.set_size(size);
        request.set_status(s);
        grpc::Status status = stub_->CreateVolumeMeta(&context,request,&response);
        if(status.ok() && !response.ret())
            return 0;
        else
            return -1;
    }
    int update_volume_status(const VOLUME_STATUS& s){
        ClientContext context;
        UpdateVolumeStatusReq request;
        UpdateVolumeStatusRes response;
        request.set_status(s);
        grpc::Status status = stub_->UpdateVolumeStatus(&context,request,&response);
        if(status.ok() && !response.ret())
            return 0;
        else
            return -1;
    }
    int read_volume_meta(const std::string& vol,VolumeInfo& info){
        ClientContext context;
        ReadVolumeMetaReq request;
        ReadVolumeMetaRes response;
        request.set_vol_id(vol);
        grpc::Status status = stub_->ReadVolumeMeta(&context,request,&response);
        if(status.ok() && !response.ret()){
            info.CopyFrom(response.info());
            return 0;
        }
        else
            return -1;
    }
    int list_volume_meta(std::list<VolumeInfo>& list){
        ClientContext context;
        ListVolumeMetaReq request;
        ListVolumeMetaRes response;
        grpc::Status status = stub_->ListVolumeMeta(&context,request,&response);
        if(status.ok() && !response.ret()){
            for(int i=0;i<response.volumes_size();++i)
                list.push_back(response.volumes(i));
            return 0;
        }
        else
            return -1;
    }
private:
    std::unique_ptr<huawei::proto::inner::VolumeInnerControl::Stub> stub_;
};
#endif
