/**********************************************
 * Copyright (c) 2017 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * File name:    volume_ctrl_client.h
 * Author:
 * Date:         Jan 20, 2017
 * Version:      1.0
 * Description:
 *
 ************************************************/
#ifndef VOLUME_CTRL_CLIENT_H_
#define VOLUME_CTRL_CLIENT_H_

#include <list>
#include <string>
#include <grpc++/grpc++.h>
#include "../common.pb.h"
#include "../volume_control.grpc.pb.h"
#include "../volume_control.pb.h"


using grpc::Channel;
using grpc::Status;
using grpc::ClientContext;
using huawei::proto::RESULT;
using huawei::proto::StatusCode;
using huawei::proto::VolumeStatus;
using huawei::proto::VolumeInfo;
using huawei::proto::sInternalError;
using huawei::proto::control::VolumeControl;
using huawei::proto::control::ListDevicesReq;
using huawei::proto::control::ListDevicesRes;
using huawei::proto::control::EnableSGReq;
using huawei::proto::control::EnableSGRes;
using huawei::proto::control::InitializeConnectionReq;
using huawei::proto::control::InitializeConnectionRes;
using huawei::proto::control::DisableSGReq;
using huawei::proto::control::DisableSGRes;
using huawei::proto::control::TerminateConnectionReq;
using huawei::proto::control::TerminateConnectionRes;
using huawei::proto::control::GetVolumeReq;
using huawei::proto::control::GetVolumeRes;
using huawei::proto::control::ListVolumesReq;
using huawei::proto::control::ListVolumesRes;

class VolumeCtrlClient
{
public:
    VolumeCtrlClient(std::shared_ptr<Channel> channel) :
            stub_(huawei::proto::control::VolumeControl::NewStub(channel))
    {
    }
    ~VolumeCtrlClient();

    StatusCode list_devices(std::list<std::string>& devices)
    {
        ListDevicesReq req;
        ClientContext context;
        ListDevicesRes res;

        Status status = stub_->ListDevices(&context, req, &res);
        if (!status.ok())
        {
            return sInternalError;
        }
        else
        {
            if (!res.status())
            {
                for (auto device : res.devices())
                {
                    devices.push_back(device);
                }
            }
            return res.status();
        }
    }

    StatusCode enable_sg(const std::string& volume_id, size_t size,
            const std::string& device)
    {
        EnableSGReq req;
        req.set_volume_id(volume_id);
        req.set_size(size);
        req.set_device(device);
        ClientContext context;
        EnableSGRes res;
    
        Status status = stub_->EnableSG(&context, req, &res);
        if (!status.ok())
        {
            return sInternalError;
        }
        else
        {
            return res.status();
        }
    }
    
    StatusCode init_conn(const std::string& vol_id) {
        ClientContext context;
        InitializeConnectionReq req;
        InitializeConnectionRes res;
        req.set_volume_id(vol_id);
    
        Status status = stub_->InitializeConnection(&context, req, &res);
        if (!status.ok()) {
            return sInternalError;
        } else {
            return res.status();
        }
    }

    StatusCode fini_conn(const std::string& vol_id) {
        ClientContext context;
        TerminateConnectionReq req;
        TerminateConnectionRes res;
        req.set_volume_id(vol_id);
    
        Status status = stub_->TerminateConnection(&context, req, &res);
        if (!status.ok()) {
            return sInternalError;
        } else {
            return res.status();
        }
    }

    StatusCode disable_sg(const std::string& volume_id)
    {
        DisableSGReq req;
        req.set_volume_id(volume_id);
        ClientContext context;
        DisableSGRes res;

        Status status = stub_->DisableSG(&context, req, &res);
        if (!status.ok())
        {
            return sInternalError;
        }
        else
        {
            return res.status();
        }
    }

    StatusCode get_volume(const std::string& volume_id, VolumeInfo& volume)
    {
        GetVolumeReq req;
        req.set_volume_id(volume_id);
        ClientContext context;
        GetVolumeRes res;

        Status status = stub_->GetVolume(&context, req, &res);
        if (!status.ok())
        {
            return sInternalError;
        }
        else
        {
            if (!res.status())
            {
                volume = res.volume();
            }
            return res.status();
        }
    }

    int list_volumes(std::list<VolumeInfo>& volumes)
    {
        ListVolumesReq req;
        ClientContext context;
        ListVolumesRes res;

        Status status = stub_->ListVolumes(&context, req, &res);
        if (!status.ok())
        {
            return sInternalError;
        }
        else
        {
            if (!res.status())
            {
                for (auto volume : res.volumes())
                {
                    volumes.push_back(volume);
                }
            }
            return res.status();
        }
    }
private:
    std::unique_ptr<huawei::proto::control::VolumeControl::Stub> stub_;
};

#endif /* VOLUME_CTRL_CLIENT_H_ */
