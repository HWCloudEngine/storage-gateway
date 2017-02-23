/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicate_proxy.h
* Author: 
* Date:         2017/02/22
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REPLICATE_PROXY_H_
#define REPLICATE_PROXY_H_
#include "../snapshot/snapshot_proxy.h"
#include "../rpc/clients/volume_inner_ctrl_client.h"
#include "../rpc/clients/replicate_inner_ctrl_client.h"
#include "../rpc/clients/volume_inner_ctrl_client.h"
class ReplicateProxy{
public:
    ReplicateProxy(const string& vol_name,
            std::shared_ptr<SnapshotProxy> snapshot_proxy);
    ~ReplicateProxy();
    // snapshot
    StatusCode create_snapshot(const string& snap_name,
            JournalMarker& marker);

    // transaction
    StatusCode create_transaction(const SnapReqHead& shead,
            const string& snap_name);
private:
    string vol_name_;
    std::shared_ptr<SnapshotProxy> snapshot_proxy_;
    std::unique_ptr<RepInnerCtrlClient> rep_inner_client_;
    std::unique_ptr<VolInnerCtrlClient> vol_inner_client_;
};

#endif
