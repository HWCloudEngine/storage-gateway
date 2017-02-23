/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    snap_reader.h
* Author: 
* Date:         2017/02/20
* Version:      1.0
* Description:  singleton snap reader wrapped a SnapshotCtrlClient
* 
************************************************/
#ifndef SNAP_READER_H_
#define SNAP_READER_H_
#include <memory>
#include "../snapshot/snapshot_type.h"
#include "../rpc/clients/snapshot_ctrl_client.h"
class SnapReader{
    std::shared_ptr<SnapshotCtrlClient> snap_ctrl_client;
public:
    static SnapReader& instance(){
        static SnapReader reader;
        return reader;
    }

    SnapReader(SnapReader&) = delete;
    SnapReader& operator=(SnapReader const&) = delete;

    std::shared_ptr<SnapshotCtrlClient> get_client(){
        return snap_ctrl_client;
    }

private:
    SnapReader(){
        snap_ctrl_client.reset(new SnapshotCtrlClient(
                grpc::CreateChannel("127.0.0.1:1111", 
                    grpc::InsecureChannelCredentials())));
    }
    ~SnapReader(){}
};
#endif
