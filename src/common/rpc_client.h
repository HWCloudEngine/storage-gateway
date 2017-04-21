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
#include <vector>
#include <list>
#include <set>
#include <map>
#include "common/block_store.h"
#include "common/rpc_policy.h"
#include "rpc/common.pb.h"
#include "rpc/volume.pb.h"
#include "rpc/backup.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/volume_inner_control.pb.h"
#include "rpc/volume_inner_control.grpc.pb.h"
#include "rpc/snapshot_inner_control.pb.h"
#include "rpc/snapshot_inner_control.grpc.pb.h"
#include "rpc/writer.grpc.pb.h"
#include "rpc/consumer.grpc.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "rpc/replicate_inner_control.pb.h"
#include "rpc/replicate_inner_control.grpc.pb.h"
#include "rpc/backup_control.pb.h"
#include "rpc/backup_control.grpc.pb.h"
#include "rpc/snapshot_control.grpc.pb.h"
#include "rpc/volume_control.pb.h"
#include "rpc/volume_control.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;

using huawei::proto::control::VolumeControl;
using huawei::proto::control::BackupControl;
using huawei::proto::control::SnapshotControl;
using huawei::proto::inner::VolumeInnerControl;
using huawei::proto::inner::SnapshotInnerControl;
using huawei::proto::inner::ReplicateInnerControl;
using huawei::proto::inner::BackupInnerControl;
using huawei::proto::Writer;
using huawei::proto::Consumer;
using huawei::proto::SnapStatus;
using huawei::proto::DiffBlocks;
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
using huawei::proto::JournalElement;
using huawei::proto::CONSUMER_TYPE;
using huawei::proto::REPLAYER;
using huawei::proto::DiffBlocks;
using huawei::proto::inner::ReadBlock;
using huawei::proto::SnapType;
using huawei::proto::SnapStatus;
using huawei::proto::SnapReqHead;
using huawei::proto::DiffBlocks;
using huawei::proto::inner::UpdateEvent;

class RpcClient {
 public:
     virtual ~RpcClient() {};
    
     void register_policy(shared_ptr<RpcPolicy> rpc_policy);
     void deregister_policy();
     
     virtual bool build_stub(const std::string& addr) = 0;

 private:
     shared_ptr<RpcPolicy> rpc_policy_;
};

class InnerRpcClient : public RpcClient {
 public:
    InnerRpcClient(const std::string& host, const int16_t& port);
    ~InnerRpcClient();
    
    bool build_stub(const std::string& addr) override;

    /*volume meta interface*/
    StatusCode create_volume(const std::string& vol, const std::string& path,
                             const uint64_t& size, const VolumeStatus& s);
    StatusCode update_volume_status(const std::string& vol,
                                    const VolumeStatus& s);
    StatusCode get_volume(const std::string& vol, VolumeInfo* info);
    StatusCode list_volume(std::list<VolumeInfo>* list);
    StatusCode delete_volume(const std::string& vol);

    /*journal meta interface*/
    bool get_writable_journals(const std::string& uuid, const std::string& vol,
                               const int limit,
                               list<JournalElement>* journal_list);
    bool seal_journals(const std::string& uuid, const std::string& vol,
                       const std::list<std::string>& list_);
    bool update_producer_marker(const std::string& uuid, const std::string& vol,
                                const JournalMarker& marker);
    bool update_multi_producer_markers(const std::string& uuid,
                            const map<std::string, JournalMarker>& markers);

    bool get_journal_marker(const std::string& vol_id, JournalMarker* marker);

    bool get_journal_list(const std::string& vol_id,
                          const JournalMarker& marker, const int limit,
                          list<JournalElement>* journal_list);

    bool update_consumer_marker(const JournalMarker& marker,
                                const std::string& vol_id);

    /*todo snapshot interface*/
    StatusCode create_snapshot(const SnapReqHead& shead,
                               const std::string& vname,
                               const std::string& sname);
    StatusCode delete_snapshot(const SnapReqHead& shead,
                               const std::string& vname,
                               const std::string& sname);
    StatusCode rollback_snapshot(const SnapReqHead& shead,
                                 const std::string& vname,
                                 const std::string& sname);
    StatusCode update_snapshot(const SnapReqHead& shead,
                               const std::string& vname,
                               const std::string& sname,
                               const UpdateEvent& sevent);
    StatusCode cow(const std::string& vname, const std::string& sname,
                   const size_t& blk_no, enum cow_op* op_type,
                   std::string* op_obj);
    StatusCode cow_update(const std::string& vname, const std::string& sname,
                          const size_t& blk_no, const std::string& blk_obj);
    StatusCode read_snapshot(const SnapReqHead& shead, const std::string& vname,
                             const std::string& sname, const off_t&  off,
                             const size_t& len, vector<ReadBlock>* read_vec);
    StatusCode diff_snapshot(const std::string& vname,
                             const std::string& first_snap,
                             const std::string& last_snap,
                             vector<DiffBlocks>* diff_vec);
    StatusCode list_snapshot(const std::string& vname, const std::string& sname,
                             vector<std::string>* snap_vec);
    StatusCode query_snapshot(const std::string& vname,
                              const std::string& sname,
                              SnapStatus* status);

    /*backup interface*/
    StatusCode create_backup(const std::string& vol_name,
                             const size_t& vol_size,
                             const std::string& backup_name,
                             const BackupOption& backup_option);
    StatusCode list_backup(const std::string& vol_name,
                           set<std::string>* backup_set);
    StatusCode get_backup(const std::string& vol_name,
                          const std::string& backup_name,
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
                                  const std::list<std::string>& peer_vols,
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
                                 const RepRole& role, bool* is_discard);

 private:
    std::string  host_;
    int16_t port_;
    shared_ptr<Channel> channel_;
    unique_ptr<VolumeInnerControl::Stub> vol_stub_;
    unique_ptr<Writer::Stub> writer_stub_;
    unique_ptr<Consumer::Stub> replayer_stub_;
    unique_ptr<SnapshotInnerControl::Stub> snap_stub_;
    unique_ptr<BackupInnerControl::Stub> backup_stub_;
    unique_ptr<ReplicateInnerControl::Stub> rep_stub_;

    CONSUMER_TYPE consumer_type;
    std::string lease_uuid;
};

class CtrlRpcClient : public RpcClient {
 public:
     CtrlRpcClient(const std::string& host, const int16_t& port);
     ~CtrlRpcClient();

    bool build_stub(const std::string& addr) override;

    StatusCode enable_sg(const std::string& volume_id, const size_t& size,
                         const std::string& device,
                         map<std::string, std::string>* driver_data);
    StatusCode disable_sg(const std::string& volume_id);
    StatusCode get_volume(const std::string& volume_id, VolumeInfo* volume);
    StatusCode list_volumes(list<VolumeInfo>* volumes);
    StatusCode list_devices(list<std::string>* devices);

    StatusCode create_snapshot(const std::string& vol_name,
                               const std::string& snap_name);
    StatusCode list_snapshot(const std::string& vol_name,
                             set<std::string>* snap_set);

    StatusCode query_snapshot(const std::string& vol_name,
                             const std::string& snap_name,
                             SnapStatus* snap_status);
    StatusCode delete_snapshot(const std::string& vol_name,
                               const std::string& snap_name);
    StatusCode rollback_snapshot(const std::string& vol_name,
                             const std::string& snap_name);
    StatusCode diff_snapshot(const std::string& vol_name,
                             const std::string& first_snap_name,
                             const std::string& last_snap_name,
                             vector<DiffBlocks>* diff);
    StatusCode read_snapshot(const std::string& vol_name,
                             const std::string& snap_name,
                             const char* buf, const size_t& len,
                             const off_t& off);

    StatusCode create_volume_from_snap(const std::string& vol_name,
                                       const std::string& snap_name,
                                       const std::string& new_vol,
                                       const std::string& new_blk);

    StatusCode query_volume_from_snap(const std::string& new_vol,
                                      VolumeStatus* new_vol_status);

    StatusCode create_backup(const std::string& vol_name,
                            const size_t& vol_size,
                            const std::string& backup_name,
                            const BackupOption& backup_option);
    StatusCode list_backup(const std::string& vol_name,
                           set<std::string>* backup_set);
    StatusCode get_backup(const std::string& vol_name,
                          const std::string& backup_name,
                          BackupStatus* backup_status);

    StatusCode delete_backup(const std::string& vol_name,
                             const std::string& backup_name);
    StatusCode restore_backup(const std::string& vol_name,
                             const std::string& backup_name,
                             const std::string& new_vol_name,
                             const size_t& new_vol_size,
                             const std::string& new_block_device);

 private:
    std::string host_;
    int16_t port_;
    shared_ptr<Channel> channel_;
    unique_ptr<VolumeControl::Stub> vol_stub_;
    unique_ptr<SnapshotControl::Stub> snap_stub_;
    unique_ptr<BackupControl::Stub> backup_stub_;
};

#endif  // SRC_COMMON_RPC_CLIENT_H_
