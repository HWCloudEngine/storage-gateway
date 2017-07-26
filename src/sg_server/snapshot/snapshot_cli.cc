/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_client.cc
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot rpc client for other module
* 
*************************************************/
#include "common/utils.h"
#include "common/config_option.h"
#include "log/log.h"
#include "../volume_inner_control.h"
#include "snapshot_cli.h"

SnapshotCtrlClient* create_snapshot_rpc_client(const std::string& vol_name) {
    VolumeInfo vol;
    auto ret = VolInnerCtrl::instance().get_volume(vol_name, vol);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "get volume:" << vol_name << " failed";
        return nullptr;
    }
    std::string client_ip = vol.attached_host();
    short client_port = g_option.ctrl_server_port;
    if (client_ip.empty()) {
        LOG_ERROR << "volume:" << vol_name << " client ip empty";
        return nullptr;
    }
    if (!network_reachable(client_ip.c_str(), client_port)) {
        LOG_ERROR << "volume:" << vol_name << " network unreachable ip:" << client_ip << " port:" << client_port; 
        return nullptr;
    }
    std::string rpc_addr = rpc_address(client_ip, client_port);
    SnapshotCtrlClient* snap_client = new SnapshotCtrlClient(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials()));
    assert(snap_client != nullptr);
    LOG_INFO << "volume:" << vol_name << " rpc client ip:" << client_ip << " snapshot rpc client create ok";
    return snap_client;
}


void destroy_snapshot_rpc_client(SnapshotCtrlClient* cli) {
    if (cli) {
        delete cli;
        LOG_INFO << " snapshot rpc client destroy ok";
    }
}
