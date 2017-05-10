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
#include "rpc/clients/volume_inner_ctrl_client.h"
#include "rpc/volume_control.pb.h"
#include "rpc/volume_control.grpc.pb.h"
#include "log/log.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace huawei::proto;

class LunTuple {
 public:
    LunTuple(uint32_t tid, std::string iqn, uint32_t lun,
            std::string volume_name, std::string block_device);
    LunTuple(const LunTuple& other);
    LunTuple& operator=(const LunTuple& other);
    ~LunTuple() {
    }

    friend std::ostream& operator<<(std::ostream& cout, const LunTuple& lun);

 public:
    uint32_t tid_;
    std::string   iqn_;
    uint32_t lun_;
    std::string   volume_name_;
    std::string   block_device_;
};


class VolumeControlBase:public control::VolumeControl::Service {
 public:
    explicit VolumeControlBase(std::shared_ptr<VolInnerCtrlClient> vol_inner_client);
    virtual ~VolumeControlBase();
    bool remove_device(const std::string& device);
    bool execute_cmd(const std::string& command, std::string& result);
    virtual bool recover_targets();
    Status ListDevices(ServerContext* context,
            const control::ListDevicesReq* req, control::ListDevicesRes* res);
    Status GetVolume(ServerContext* context, const control::GetVolumeReq* req,
            control::GetVolumeRes* res);
    Status ListVolumes(ServerContext* context,
            const control::ListVolumesReq* req, control::ListVolumesRes* res);
    virtual Status EnableSG(ServerContext* context, const control::EnableSGReq* req,
            control::EnableSGRes* res);
    virtual Status DisableSG(ServerContext* context, const control::DisableSGReq* req,
            control::DisableSGRes* res);

 private:
    std::shared_ptr<VolInnerCtrlClient> vol_inner_client_;

};

class VolumeControlImpl final: public VolumeControlBase {
 public:
    VolumeControlImpl(const std::string& host, const std::string& port,
            std::shared_ptr<VolInnerCtrlClient> vol_inner_client);
    virtual ~VolumeControlImpl();
    Status EnableSG(ServerContext* context, const control::EnableSGReq* req,
            control::EnableSGRes* res);
    Status DisableSG(ServerContext* context, const control::DisableSGReq* req,
            control::DisableSGRes* res);
    bool recover_targets();

 private:
    bool enable_sg(const std::string vol_name, const std::string dev_name, const size_t dev_size,
                   std::string& iqn_name, bool recover = true);
    std::string get_target_iqn(const std::string& volume_id);
    bool generate_config(const std::string& volume_id,
                         const std::string& device, const std::string& target_iqn,
                         std::string& config);
    bool persist_config(const std::string& volume_id,
                        const std::string& config);
    bool remove_config(const std::string& volume_id);

    bool add_target(const LunTuple& lun);
    bool remove_target(uint32_t tid);
    bool add_lun(const LunTuple& lun);
    bool remove_lun(const LunTuple& lun);
    bool acl_bind(const LunTuple& lun);
    bool acl_unbind(const LunTuple& lun);

    /*recover targets*/
    bool recover_target(const char* vol_name);

    std::shared_ptr<VolInnerCtrlClient> vol_inner_client_;
    std::string host_;
    std::string port_;
    std::string target_path_;
    std::string target_prefix_;
    static int tid_id;
    static int lun_id;
    std::map<std::string, LunTuple> tgt_luns_;
};

#endif  // SRC_SG_CLIENT_CONTROL_CONTROL_VOLUME_H_
