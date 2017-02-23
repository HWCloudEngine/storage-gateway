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
#include "../../log/log.h"
#include "../volume_inner_control.grpc.pb.h"
using grpc::ClientContext;
using huawei::proto::inner::CreateVolumeReq;
using huawei::proto::inner::CreateVolumeRes;
using huawei::proto::inner::UpdateVolumeStatusReq;
using huawei::proto::inner::UpdateVolumeStatusRes;
using huawei::proto::inner::GetVolumeReq;
using huawei::proto::inner::GetVolumeRes;
using huawei::proto::inner::ListVolumeReq;
using huawei::proto::inner::ListVolumeRes;
using huawei::proto::inner::DeleteVolumeReq;
using huawei::proto::inner::DeleteVolumeRes;
using huawei::proto::VolumeInfo;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeStatus;
using huawei::proto::StatusCode;
using huawei::proto::sInternalError;
class VolInnerCtrlClient{
public:
    VolInnerCtrlClient(std::shared_ptr<grpc::Channel> channel):
        stub_(huawei::proto::inner::VolumeInnerControl::NewStub(channel)){
    }
    ~VolInnerCtrlClient(){}
    StatusCode create_volume(const std::string& vol,const std::string& path,
            const uint64_t& size, const VolumeStatus& s){
        ClientContext context;
        CreateVolumeReq request;
        CreateVolumeRes response;
        request.set_vol_id(vol);
        request.set_path(path);
        request.set_size(size);
        request.set_status(s);
        grpc::Status status = stub_->CreateVolume(&context,request,&response);
        if(status.ok())
            return response.status();
        else{
            LOG_ERROR << "create volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }
    StatusCode update_volume_status(const std::string& vol,
            const VolumeStatus& s){
        ClientContext context;
        UpdateVolumeStatusReq request;
        UpdateVolumeStatusRes response;
        request.set_status(s);
        request.set_vol_id(vol);
        grpc::Status status = stub_->UpdateVolumeStatus(&context,request,&response);
        if(status.ok())
            return response.status();
        else{
            LOG_ERROR << "create volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }
    StatusCode get_volume(const std::string& vol,VolumeInfo& info){
        ClientContext context;
        GetVolumeReq request;
        GetVolumeRes response;
        request.set_vol_id(vol);
        grpc::Status status = stub_->GetVolume(&context,request,&response);
        if(status.ok())
            return response.status();
        else{
            LOG_ERROR << "get volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }
    StatusCode list_volume(std::list<VolumeInfo>& list){
        ClientContext context;
        ListVolumeReq request;
        ListVolumeRes response;
        grpc::Status status = stub_->ListVolume(&context,request,&response);
        if(!status.ok()){
            LOG_ERROR << "list volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
        if(!response.status()){
            for(int i=0;i<response.volumes_size();++i)
                list.push_back(response.volumes(i));
        }
        return response.status();
    }
    StatusCode delete_volume(const std::string& vol){
        ClientContext context;
        DeleteVolumeReq request;
        DeleteVolumeRes response;
        request.set_vol_id(vol);
        grpc::Status status = stub_->DeleteVolume(&context,request,&response);
        if(status.ok())
            return response.status();
        else{
            LOG_ERROR << "delete volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }
private:
    std::unique_ptr<huawei::proto::inner::VolumeInnerControl::Stub> stub_;
};
#endif
