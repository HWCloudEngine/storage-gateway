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
#include "snapshot/snapshot_proxy.h"
#include "../rpc/clients/volume_inner_ctrl_client.h"
#include "../rpc/clients/replicate_inner_ctrl_client.h"
#include "../rpc/clients/volume_inner_ctrl_client.h"
using huawei::proto::RepRole;

class ReplicateProxy{
public:
    ReplicateProxy(const string& vol_name,const size_t& vol_size,
            std::shared_ptr<SnapshotProxy> snapshot_proxy);
    ~ReplicateProxy();
    // snapshot
    StatusCode create_snapshot(const string& operate_id,JournalMarker& marker);

    // transaction
    StatusCode create_transaction(const SnapReqHead& shead,
            const string& snap_name, const RepRole& role);

    // mapings between replicate operate uuid and snapshot name
    string snap_name_to_operate_uuid(const string& snap_name);
    string operate_uuid_to_snap_name(const string& operate_id);

private:
    string vol_name_;
    size_t vol_size_;
    std::shared_ptr<SnapshotProxy> snapshot_proxy_;
    std::unique_ptr<RepInnerCtrlClient> rep_inner_client_;
};

#endif
