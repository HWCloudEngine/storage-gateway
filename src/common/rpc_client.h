/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rpc_server.hpp
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef SRC_COMMON_RPC_CLIENT_H_
#define SRC_COMMON_RPC_CLIENT_H_
#include <grpc++/grpc++.h>
#include <string>
#include <list>
#include <set>
#include "common/block_store.h"
#include "rpc/common.pb.h"
#include "rpc/volume.pb.h"
#include "rpc/backup.pb.h"
#include "rpc/volume_inner_control.pb.h"
#include "rpc/volume_inner_control.grpc.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "rpc/replicate_inner_control.pb.h"
#include "rpc/replicate_inner_control.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;

using huawei::proto::inner::VolumeInnerControl;
using huawei::proto::inner::ReplicateInnerControl;
using huawei::proto::inner::BackupInnerControl;

using huawei::proto::VolumeInfo;
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeStatus;
using huawei::proto::StatusCode;
using huawei::proto::StatusCode;
using huawei::proto::BackupMode;
using huawei::proto::BackupStatus;
using huawei::proto::BackupOption;
using huawei::proto::RepRole;
using huawei::proto::JournalMarker;

class RpcClient {
 public:
    RpcClient(const std::string& host, const int16_t& port);
    ~RpcClient();

    /*volume meta interface*/
    StatusCode create_volume(const std::string& vol, const std::string& path,
                             const uint64_t& size, const VolumeStatus& s);
    StatusCode update_volume_status(const std::string& vol,
                                    const VolumeStatus& s);
    StatusCode get_volume(const std::string& vol, VolumeInfo* info);
    StatusCode list_volume(std::list<VolumeInfo>* list);
    StatusCode delete_volume(const std::string& vol);

    /*todo snapshot interface*/

    /*backup interface*/
    StatusCode create_backup(const std::string& vol_name,
                             const size_t& vol_size,
                             const std::string& backup_name,
                             const BackupOption& backup_option);
    StatusCode list_backup(const std::string& vol_name, set<string>* backup_set);
    StatusCode get_backup(const std::string& vol_name, const string& backup_name,
                         BackupStatus* backup_status);
    StatusCode delete_backup(const std::string& vol_name,
                             const std::string& backup_name);
    StatusCode restore_backup(const std::string& vol_name,
                              const std::string& backup_name,
                              const std::string& new_vol_name,
                              const size_t& new_vol_size,
                              const std::string& new_block_device,
                              BlockStore* block_store);
    /*replicate interface*/
    StatusCode create_replication(const std::string& op_id,
                                  const std::string& rep_id,
                                  const std::string& local_vol,
                                  const std::list<string>& peer_vols,
                                  const RepRole& role);
    StatusCode enable_replication(const std::string& op_id,
                                  const std::string& vol_id,
                                  const RepRole& role,
                                  const JournalMarker& marker,
                                  const std::string& snap_id);
    StatusCode disable_replication(const std::string& op_id,
                                   const std::string& vol_id,
                                   const RepRole& role,
                                   const JournalMarker& marker,
                                   const std::string& snap_id);
    StatusCode failover_replication(const std::string& op_id,
                                    const std::string& vol_id,
                                    const RepRole& role,
                                    const JournalMarker& marker,
                                    const bool& need_sync,
                                    const std::string& snap_id);
    StatusCode reverse_replication(const std::string& op_id,
                                   const std::string& vol_id,
                                   const RepRole& role);
    StatusCode delete_replication(const std::string& op_id,
                                  const std::string& vol_id,
                                  const RepRole& role);
    StatusCode report_checkpoint(const std::string& op_id,
                                 const std::string& vol_id,
                                 const RepRole& role, bool& is_discard);

 private:
    std::string  host_;
    int16_t port_;
    shared_ptr<Channel> channel_;
    unique_ptr<VolumeInnerControl::Stub> vol_stub_;
    // unique_ptr<SnapshotInnerControl::Stub> snap_stub_;
    unique_ptr<BackupInnerControl::Stub> backup_stub_;
    unique_ptr<ReplicateInnerControl::Stub> rep_stub_;
};

#endif  // SRC_COMMON_RPC_CLIENT_H_


