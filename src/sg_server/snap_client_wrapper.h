/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    snap_client_wrapper.h
* Author: 
* Date:         2017/02/20
* Version:      1.0
* Description:  singleton snap reader wrapped a SnapshotCtrlClient
* 
************************************************/
#ifndef SNAP_CLIENT_WRAPPER_H_
#define SNAP_CLIENT_WRAPPER_H_
#include <memory>
#include "../rpc/clients/snapshot_ctrl_client.h"
class SnapClientWrapper{
    std::shared_ptr<SnapshotCtrlClient> snap_ctrl_client;
public:
    static SnapClientWrapper& instance(){
        static SnapClientWrapper reader;
        return reader;
    }

    SnapClientWrapper(SnapClientWrapper&) = delete;
    SnapClientWrapper& operator=(SnapClientWrapper const&) = delete;

    std::shared_ptr<SnapshotCtrlClient> get_client(){
        return snap_ctrl_client;
    }

private:
    SnapClientWrapper(){
        // TODO: get sg_cleint rpc port from config file
        snap_ctrl_client.reset(new SnapshotCtrlClient(
                grpc::CreateChannel("127.0.0.1:1111", 
                    grpc::InsecureChannelCredentials())));
    }
    ~SnapClientWrapper(){}
};
#endif
