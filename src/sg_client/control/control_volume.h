/**********************************************
 * Copyright (c) 2017 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * File name:    volume_control.h
 * Author:
 * Date:         Jan 20, 2017
 * Version:      1.0
 * Description:
 *
 ************************************************/
#ifndef VOLUME_CONTROL_H_
#define VOLUME_CONTROL_H_

#include <list>
#include <string>
#include <grpc++/grpc++.h>
#include "common/config.h"
#include "rpc/common.pb.h"
#include "rpc/clients/volume_inner_ctrl_client.h"
#include "rpc/volume_control.pb.h"
#include "rpc/volume_control.grpc.pb.h"
#include "log/log.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace huawei::proto;

class VolumeControlImpl final: public control::VolumeControl::Service
{

public:
    VolumeControlImpl(const Configure& conf, const std::string& host, const std::string& port,
            std::shared_ptr<VolInnerCtrlClient> vol_inner_client);

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
    
private:
    bool execute_cmd(const std::string& command, std::string& result);

    bool create_volume(const std::string& volume_id, size_t size,
            const std::string& device);
    std::string get_target_iqn(const std::string& volume_id);
    bool generate_config(const std::string& volume_id,
            const std::string& device, const std::string& target_iqn,
            std::string& config);
    bool persist_config(const std::string& volume_id,
            const std::string& config);
    bool update_target(const std::string& target_iqn);
    bool get_target(const std::string& target_iqn);
    bool remove_target(const std::string& target_iqn);
    bool remove_config(const std::string& volume_id);
    bool remove_device(const std::string& device);
    bool delete_volume(const std::string& volume_id);

    bool update_volume_status(const std::string& volume_id,
            const huawei::proto::VolumeStatus& status);
    
    Configure conf_;
    std::shared_ptr<VolInnerCtrlClient> vol_inner_client_;
    std::string host_;
    std::string port_;
    std::string target_path_;
    std::string target_prefix_;
};

#endif /* VOLUME_CONTROL_H_ */
