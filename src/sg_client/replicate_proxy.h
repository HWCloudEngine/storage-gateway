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
#include "common/config_option.h"
#include "snapshot/snapshot_proxy.h"
#include "../rpc/clients/volume_inner_ctrl_client.h"
#include "../rpc/clients/replicate_inner_ctrl_client.h"
#include "../rpc/clients/volume_inner_ctrl_client.h"
#include "common/locks.h"
using huawei::proto::RepRole;
using huawei::proto::SnapScene;
using huawei::proto::SnapType;

class ReplicateProxy {
 public:
    ReplicateProxy(const string& vol_name, const size_t& vol_size,
                   std::shared_ptr<SnapshotProxy> snapshot_proxy);
    ~ReplicateProxy();
    // snapshot
    StatusCode create_snapshot(const string& operate_id,
            JournalMarker& marker,const string& checkpoint_id,
            const SnapScene& snap_scene,const SnapType& snap_type);

    // transaction
    StatusCode create_transaction(const SnapReqHead& shead,
            const string& snap_name, const RepRole& role);

    // sync operation
    void add_sync_item(const std::string& actor,const std::string& action);
    void delete_sync_item(const std::string& actor);
    bool is_sync_item_exist(const std::string& actor);

 private:
    string vol_name_;
    size_t vol_size_;
    std::shared_ptr<SnapshotProxy> snapshot_proxy_;
    std::unique_ptr<RepInnerCtrlClient> rep_inner_client_;
    SharedMutex map_mtx_;
    std::map<std::string,std::string> sync_map_;
};

#endif
