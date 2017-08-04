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
using huawei::proto::inner::UpdateVolumeReq;
using huawei::proto::inner::UpdateVolumeRes;
using huawei::proto::inner::GetVolumeReq;
using huawei::proto::inner::GetVolumeRes;
using huawei::proto::inner::ListVolumeReq;
using huawei::proto::inner::ListVolumeRes;
using huawei::proto::inner::DeleteVolumeReq;
using huawei::proto::inner::DeleteVolumeRes;
using huawei::proto::inner::VolumeInnerControl;
using huawei::proto::VolumeInfo;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeStatus;
using huawei::proto::StatusCode;
using huawei::proto::sInternalError;
class VolInnerCtrlClient{
public:
    explicit VolInnerCtrlClient(){
    }
    ~VolInnerCtrlClient(){}
    static StatusCode create_volume(const std::string& vol,const std::string& path,
            const uint64_t& size, const VolumeStatus& s, const std::string& attached_host="127.0.0.1"){
        ClientContext context;
        CreateVolumeReq request;
        CreateVolumeRes response;
        request.set_vol_id(vol);
        request.set_path(path);
        request.set_size(size);
        request.set_status(s);
        request.set_attached_host(attached_host);
        grpc::Status status = stub_->CreateVolume(&context,request,&response);
        if(status.ok())
            return response.status();
        else{
            LOG_ERROR << "create volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }

    static StatusCode get_volume(const std::string& vol,VolumeInfo& info){
        ClientContext context;
        GetVolumeReq request;
        GetVolumeRes response;
        request.set_vol_id(vol);
        grpc::Status status = stub_->GetVolume(&context,request,&response);
        if(status.ok()){
            info.CopyFrom(response.info()); 
            return response.status();
        }
        else{
            LOG_ERROR << "get volume failed:" 
                << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }

    static StatusCode list_volume(std::list<VolumeInfo>& list){
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
    static StatusCode delete_volume(const std::string& vol){
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

    static StatusCode update_volume(const UpdateVolumeReq& request){
        ClientContext context;
        UpdateVolumeRes response;
        grpc::Status status = stub_->UpdateVolume(&context,request,&response);
        if(status.ok())
            return response.status();
        else{
            LOG_ERROR << "update volume path failed:"
                      << status.error_message() << ",code:" << status.error_code();
            return sInternalError;
        }
    }

    static void init(std::shared_ptr<grpc::Channel> channel){
        stub_.reset(new VolumeInnerControl::Stub(channel));
    }

private:
    static std::unique_ptr<VolumeInnerControl::Stub> stub_;
};
#endif
