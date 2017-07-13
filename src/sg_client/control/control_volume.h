/**********************************************
 * Copyright (c) 2017 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * File name:    control_volume.h
 * Author:
 * Date:         Jan 20, 2017
 * Version:      1.0
 * Description:  rpc volume control for sgs server
 *
 ************************************************/
#ifndef SRC_SG_CLIENT_CONTROL_CONTROL_VOLUME_H_
#define SRC_SG_CLIENT_CONTROL_CONTROL_VOLUME_H_
#include <iostream>
#include <list>
#include <map>
#include <string>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/clients/rpc_client.h"
#include "rpc/volume_control.pb.h"
#include "rpc/volume_control.grpc.pb.h"
#include "log/log.h"
#include "control_iscsi.h"
#include "control_agent.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace huawei::proto;

class VolumeManager;

class VolumeControlImpl:public control::VolumeControl::Service {
public:
    VolumeControlImpl(const std::string& host, const std::string& port,
                      VolumeManager& vol_manager);
    ~VolumeControlImpl();
    Status ListDevices(ServerContext* context,
                       const control::ListDevicesReq* req, control::ListDevicesRes* res);
    Status EnableSG(ServerContext* context, const control::EnableSGReq* req,
                    control::EnableSGRes* res);
    Status DisableSG(ServerContext* context, const control::DisableSGReq* req,
                     control::DisableSGRes* res);
    Status GetVolume(ServerContext* context, const control::GetVolumeReq* req,
                     control::GetVolumeRes* res);
    Status ListVolumes(ServerContext* context,
                       const control::ListVolumesReq* req, control::ListVolumesRes* res);
    Status InitializeConnection(ServerContext* context,
                                const control::InitializeConnectionReq* req,
                                control::InitializeConnectionRes* res);
    Status TerminateConnection(ServerContext* context,
                               const control::TerminateConnectionReq* req,
                               control::TerminateConnectionRes* res);
    Status AttachVolume(ServerContext* context,
                        const control::AttachVolumeReq* req,
                        control::AttachVolumeRes* res);
    Status DetachVolume(ServerContext* context,
                        const control::DetachVolumeReq* req,
                        control::DetachVolumeRes* res);
    bool recover_targets();

private:
    bool execute_cmd(const std::string& command, std::string& result);
    ISCSIControl* iscsi_control_ptr{nullptr};
    AgentControl* agent_control_ptr{nullptr};

    std::string host_;
    std::string port_;
    VolumeManager& vol_manager_;
};

#endif  // SRC_SG_CLIENT_CONTROL_CONTROL_VOLUME_H_
