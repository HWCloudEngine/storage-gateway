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
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "utils.h"
#include "common/define.h"
#include "log/log.h"
#include "rpc_client.h"

// todo these type add such as In
using huawei::proto::inner::CreateVolumeReq;
using huawei::proto::inner::CreateVolumeRes;
using huawei::proto::inner::UpdateVolumeStatusReq;
using huawei::proto::inner::UpdateVolumeStatusRes;
using huawei::proto::inner::GetVolumeReq;
using huawei::proto::inner::GetVolumeRes;
using huawei::proto::inner::ListVolumeReq;
using huawei::proto::inner::ListVolumeRes;
using huawei::proto::inner::DeleteVolumeReq;
using huawei::proto::inner::DeleteVolumeRes;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
using huawei::proto::UpdateProducerMarkerRequest;
using huawei::proto::UpdateProducerMarkerResponse;
using huawei::proto::UpdateMultiProducerMarkersRequest;
using huawei::proto::UpdateMultiProducerMarkersResponse;
using huawei::proto::GetJournalListRequest;
using huawei::proto::GetJournalListResponse;
using huawei::proto::GetJournalMarkerRequest;
using huawei::proto::GetJournalMarkerResponse;
using huawei::proto::UpdateConsumerMarkerRequest;
using huawei::proto::UpdateConsumerMarkerResponse;
using huawei::proto::inner::CreateReq;
using huawei::proto::inner::CreateAck;
using huawei::proto::inner::ListReq;
using huawei::proto::inner::ListAck;
using huawei::proto::inner::QueryReq;
using huawei::proto::inner::QueryAck;
using huawei::proto::inner::DeleteReq;
using huawei::proto::inner::DeleteAck;
using huawei::proto::inner::CowReq;
using huawei::proto::inner::CowAck;
using huawei::proto::inner::UpdateReq;
using huawei::proto::inner::UpdateAck;
using huawei::proto::inner::CowUpdateReq;
using huawei::proto::inner::CowUpdateAck;
using huawei::proto::inner::RollbackReq;
using huawei::proto::inner::RollbackAck;
using huawei::proto::inner::DiffReq;
using huawei::proto::inner::DiffAck;
using huawei::proto::inner::ReadReq;
using huawei::proto::inner::ReadAck;
using huawei::proto::inner::SyncReq;
using huawei::proto::inner::SyncAck;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;
using huawei::proto::inner::UpdateEvent;
using huawei::proto::inner::BackupInnerControl;
using huawei::proto::inner::CreateBackupInReq;
using huawei::proto::inner::CreateBackupInAck;
using huawei::proto::inner::ListBackupInReq;
using huawei::proto::inner::ListBackupInAck;
using huawei::proto::inner::GetBackupInReq;
using huawei::proto::inner::GetBackupInAck;
using huawei::proto::inner::DeleteBackupInReq;
using huawei::proto::inner::DeleteBackupInAck;
using huawei::proto::inner::RestoreBackupInReq;
using huawei::proto::inner::RestoreBackupInAck;
using huawei::proto::inner::CreateReplicationInnerReq;
using huawei::proto::inner::EnableReplicationInnerReq;
using huawei::proto::inner::DisableReplicationInnerReq;
using huawei::proto::inner::FailoverReplicationInnerReq;
using huawei::proto::inner::ReverseReplicationInnerReq;
using huawei::proto::inner::DeleteReplicationInnerReq;
using huawei::proto::inner::ReplicationInnerCommonRes;
using huawei::proto::inner::ReportCheckpointReq;
using huawei::proto::inner::ReportCheckpointRes;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::JournalMarker;
using huawei::proto::JournalElement;

using huawei::proto::control::VolumeControl;
using huawei::proto::control::ListDevicesReq;
using huawei::proto::control::ListDevicesRes;
using huawei::proto::control::EnableSGReq;
using huawei::proto::control::EnableSGRes;
using huawei::proto::control::DisableSGReq;
using huawei::proto::control::DisableSGRes;
// using huawei::proto::control::GetVolumeReq;
// using huawei::proto::control::GetVolumeRes;
using huawei::proto::control::ListVolumesReq;
using huawei::proto::control::ListVolumesRes;
using huawei::proto::control::SnapshotControl;
using huawei::proto::control::CreateSnapshotReq;
using huawei::proto::control::CreateSnapshotAck;
using huawei::proto::control::ListSnapshotReq;
using huawei::proto::control::ListSnapshotAck;
using huawei::proto::control::QuerySnapshotReq;
using huawei::proto::control::QuerySnapshotAck;
using huawei::proto::control::DeleteSnapshotReq;
using huawei::proto::control::DeleteSnapshotAck;
using huawei::proto::control::RollbackSnapshotReq;
using huawei::proto::control::RollbackSnapshotAck;
using huawei::proto::control::DiffSnapshotReq;
using huawei::proto::control::DiffSnapshotAck;
using huawei::proto::control::ReadSnapshotReq;
using huawei::proto::control::ReadSnapshotAck;
using huawei::proto::control::CreateVolumeFromSnapReq;
using huawei::proto::control::CreateVolumeFromSnapAck;
using huawei::proto::control::QueryVolumeFromSnapReq;
using huawei::proto::control::QueryVolumeFromSnapAck;
using huawei::proto::control::BackupControl;
using huawei::proto::control::CreateBackupReq;
using huawei::proto::control::CreateBackupAck;
using huawei::proto::control::ListBackupReq;
using huawei::proto::control::ListBackupAck;
using huawei::proto::control::GetBackupReq;
using huawei::proto::control::GetBackupAck;
using huawei::proto::control::DeleteBackupReq;
using huawei::proto::control::DeleteBackupAck;
using huawei::proto::control::RestoreBackupReq;
using huawei::proto::control::RestoreBackupAck;

#define RPC_CALL(stub, fname, vname, req, rep)  do {         \
    ClientContext ctx;                                       \
    grpc::Status status;                                     \
    while (true) {                                           \
        status = stub->fname(&ctx, req, &rep);               \
        if (status.ok()) {                                   \
            break;                                           \
        }                                                    \
        std::string addr = rpc_policy_->pick_host(vname);    \
        if (!addr.empty()) {                                 \
            build_stub(addr);                                \
        }                                                    \
    }                                                        \
} while (0)

void RpcClient::register_policy(shared_ptr<RpcPolicy> rpc_policy) {
    rpc_policy_ = rpc_policy;
}

void RpcClient::deregister_policy() {
    rpc_policy_.reset();
}

InnerRpcClient::InnerRpcClient(const std::string& host, const int16_t& port) {
    host_ = host;
    port_ = port;
    std::string addr = rpc_server_address(host, port);
    build_stub(addr);

    lease_uuid = "lease-uuid";
    consumer_type = REPLAYER;
}

InnerRpcClient::~InnerRpcClient() {
}

bool InnerRpcClient::build_stub(const std::string& addr) {
    channel_ = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    vol_stub_ = VolumeInnerControl::NewStub(channel_);
    writer_stub_ = Writer::NewStub(channel_);
    replayer_stub_ = Consumer::NewStub(channel_);
    backup_stub_ = BackupInnerControl::NewStub(channel_);
    snap_stub_ = SnapshotInnerControl::NewStub(channel_);
    backup_stub_ = BackupInnerControl::NewStub(channel_);
    rep_stub_ = ReplicateInnerControl::NewStub(channel_);
    return true;
}

StatusCode InnerRpcClient::create_volume(const std::string& vol,
                                         const std::string& path,
                                         const uint64_t& size,
                                         const VolumeStatus& s) {
    CreateVolumeReq request;
    CreateVolumeRes response;
    request.set_vol_id(vol);
    request.set_path(path);
    request.set_size(size);
    request.set_status(s);

    RPC_CALL(vol_stub_, CreateVolume, vol, request, response);
    return response.status();
}

StatusCode InnerRpcClient::update_volume_status(const std::string& vol,
                                                const VolumeStatus& s) {
    UpdateVolumeStatusReq request;
    UpdateVolumeStatusRes response;
    request.set_status(s);
    request.set_vol_id(vol);
    RPC_CALL(vol_stub_, UpdateVolumeStatus, vol, request, response);
    return response.status();
}

StatusCode InnerRpcClient::get_volume(const std::string& vol,
                                      VolumeInfo* info) {
    GetVolumeReq request;
    GetVolumeRes response;
    request.set_vol_id(vol);
    RPC_CALL(vol_stub_, GetVolume, vol, request, response);
    if (!response.status()) {
        info->CopyFrom(response.info());
    }
    return response.status();
}

StatusCode InnerRpcClient::list_volume(std::list<VolumeInfo>* list) {
    ListVolumeReq request;
    ListVolumeRes response;
    std::string vol = "";
    RPC_CALL(vol_stub_, ListVolume, vol, request, response);
    if (!response.status()) {
        for (int i = 0; i < response.volumes_size(); ++i) {
            if (list) {
                list->push_back(response.volumes(i));
            }
        }
    }
    return response.status();
}

StatusCode InnerRpcClient::delete_volume(const std::string& vol) {
    DeleteVolumeReq request;
    DeleteVolumeRes response;
    request.set_vol_id(vol);

    RPC_CALL(vol_stub_, DeleteVolume, vol, request, response);
    return response.status();
}

StatusCode InnerRpcClient::get_writable_journals(const std::string& uuid,
                                                 const std::string& vol,
                                                 const int limit,
                                      list<JournalElement>* journal_list) {
    GetWriteableJournalsRequest request;
    GetWriteableJournalsResponse response;
    request.set_uuid(uuid);
    request.set_vol_id(vol);
    request.set_limits(limit);

    RPC_CALL(writer_stub_, GetWriteableJournals, vol, request, response);
    RESULT result = response.result();
    if (result == DRS_OK) {
        for (int i = 0; i < response.journals_size(); i++) {
            journal_list->push_back(response.journals(i));
        }
    }
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::seal_journals(const std::string& uuid,
                                         const std::string& vol,
                                const std::list<std::string>& journal_list) {
    SealJournalsRequest request;
    SealJournalsResponse response;
    request.set_uuid(uuid);
    request.set_vol_id(vol);
    for (auto it : journal_list) {
        request.add_journals(it);
    }

    RPC_CALL(writer_stub_, SealJournals, vol, request, response);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::update_producer_marker(const std::string& uuid,
        const std::string& vol, const JournalMarker& marker) {
    UpdateProducerMarkerRequest request;
    UpdateProducerMarkerResponse response;
    request.set_uuid(uuid);
    request.set_vol_id(vol);
    request.mutable_marker()->CopyFrom(marker);

    RPC_CALL(writer_stub_, UpdateProducerMarker, vol, request, response);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::update_multi_producer_markers(const std::string& uuid,
        const map<std::string, JournalMarker>& markers) {
    UpdateMultiProducerMarkersRequest request;
    UpdateMultiProducerMarkersResponse response;

    request.set_uuid(uuid);
    for (auto it = markers.begin(); it != markers.end(); it++) {
        (*request.mutable_markers())[it->first] = it->second;
    }
    
    std::string vol_name;
    RPC_CALL(writer_stub_, UpdateMultiProducerMarkers, vol_name, request, response);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::get_journal_marker(const std::string& vol_id,
                                        JournalMarker* marker_) {
    GetJournalMarkerRequest request;
    GetJournalMarkerResponse response;
    request.set_vol_id(vol_id);
    request.set_uuid(lease_uuid);
    request.set_type(consumer_type);
    ClientContext context;

    RPC_CALL(replayer_stub_, GetJournalMarker, vol_id, request, response);
    RESULT result = response.result();
    if (result == DRS_OK) {
        marker_->CopyFrom(response.marker());
    }
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::get_journal_list(const std::string& vol_id,
                               const JournalMarker& marker,
                               const int limit,
                               list<JournalElement>* journal_list) {
    GetJournalListRequest request;
    GetJournalListResponse response;
    request.set_limit(limit);
    request.set_vol_id(vol_id);
    request.set_uuid(lease_uuid);
    request.set_type(consumer_type);
    (request.mutable_marker())->CopyFrom(marker);

    RPC_CALL(replayer_stub_, GetJournalList, vol_id, request, response);
    RESULT result = response.result();
    if (result == DRS_OK) {
        for (int i = 0; i < response.journals_size(); i++) {
            journal_list->push_back(response.journals(i));
        }
    }
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::update_consumer_marker(const JournalMarker& marker,
                                       const std::string& vol_id) {
    UpdateConsumerMarkerRequest request;
    UpdateConsumerMarkerResponse response;
    request.set_uuid(lease_uuid);
    request.set_vol_id(vol_id);
    request.set_type(consumer_type);
    (request.mutable_marker())->CopyFrom(marker);

    RPC_CALL(replayer_stub_, UpdateConsumerMarker, vol_id, request, response);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::create_snapshot(const SnapReqHead& shead,
                                           const std::string& vname,
                                           const std::string& sname) {
    LOG_INFO << "create snap_name:" << sname;

    CreateReq ireq;
    CreateAck iack;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    
    RPC_CALL(snap_stub_, Create, vname, ireq, iack);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::delete_snapshot(const SnapReqHead& shead,
                                           const std::string& vname,
                                           const std::string& sname) {
    LOG_INFO << "delete snap_name:" << sname;
    DeleteReq ireq;
    DeleteAck iack;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);

    RPC_CALL(snap_stub_, Delete, vname, ireq, iack);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::rollback_snapshot(const SnapReqHead& shead,
                                             const std::string& vname,
                                             const std::string& sname) {
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::update_snapshot(const SnapReqHead& shead,
                                           const std::string& vname,
                                           const std::string& sname,
                                           const UpdateEvent& sevent) {
    LOG_INFO << "update snap_name:" << sname;
    UpdateReq ireq;
    UpdateAck iack;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_snap_event(sevent);

    RPC_CALL(snap_stub_, Update, vname, ireq, iack);
    return StatusCode::sOk;
}

// todo reture cow object
StatusCode InnerRpcClient::cow(const std::string& vname,
                               const std::string& sname,
                               const size_t& blk_no,
                               enum cow_op* op_type, std::string* op_obj) {
    LOG_INFO << "cow snap_name:" << sname;
    CowReq ireq;
    CowAck iack;
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_blk_no(blk_no);

    RPC_CALL(snap_stub_, CowOp, vname, ireq, iack);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::cow_update(const std::string& vname,
                                     const std::string& sname,
                                     const size_t& blk_no,
                                     const std::string& blk_obj) {
    LOG_INFO << "cow update snap_name:" << sname;
    CowUpdateReq ireq;
    CowUpdateAck iack;
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_blk_no(blk_no);
    ireq.set_cow_blk_object(blk_obj);

    RPC_CALL(snap_stub_, CowUpdate, vname, ireq, iack);
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::read_snapshot(const SnapReqHead& shead,
                                    const std::string& vname,
                                    const std::string& sname,
                                    const off_t&  off, const size_t& len,
                                    vector<ReadBlock>* read_vec) {
    LOG_INFO << "read snap_name:" << sname << " off:" << off << " len:" << len;
    ReadReq ireq;
    ReadAck iack;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_off(off);
    ireq.set_len(len);

    RPC_CALL(snap_stub_, Read, vname, ireq, iack);
    if (!iack.header().status()) {
        for (int i = 0; i < iack.read_blocks_size(); i++) {
            read_vec->push_back(iack.read_blocks(i));
        }
    }
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::diff_snapshot(const std::string& vname,
                                         const std::string& first_snap,
                                         const std::string& last_snap,
                                         vector<DiffBlocks>* diff_vec) {
    LOG_INFO << "diff vname:" << vname << " first_snap:" << first_snap
             << " last_snap:" << last_snap;

    DiffReq ireq;
    DiffAck iack;
    ireq.set_vol_name(vname);
    ireq.set_first_snap_name(first_snap);
    ireq.set_last_snap_name(last_snap);

    RPC_CALL(snap_stub_, Diff, vname, ireq, iack);
    int diff_blocks_num = iack.diff_blocks_size();
    for (int i = 0; i < diff_blocks_num; i++) {
        DiffBlocks  idiff_blocks = iack.diff_blocks(i);
        diff_vec->push_back(idiff_blocks);
    }
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::list_snapshot(const std::string& vname,
                                         const std::string& sname,
                                         vector<std::string>* snap_vec) {
    LOG_INFO << "list snapshot vname:" << vname;
    ListReq ireq;
    ListAck iack;
    ireq.set_vol_name(vname);

    RPC_CALL(snap_stub_, List, vname, ireq, iack);
    int snap_num = iack.snap_name_size();
    for (int i = 0; i < snap_num; i++) {
       snap_vec->push_back(iack.snap_name(i));
    }
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::query_snapshot(const std::string& vname,
                                         const std::string& sname,
                                         SnapStatus* status) {
    LOG_INFO << "query snapshot vname:" << vname << " sname:" << sname;
    QueryReq ireq;
    QueryAck iack;
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);

    RPC_CALL(snap_stub_, Query, vname, ireq, iack);
    *status = iack.snap_status();
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::create_backup(const std::string& vol_name,
                                    const size_t& vol_size,
                                    const std::string& backup_name,
                                    const BackupOption& backup_option) {
    CreateBackupInReq req;
    CreateBackupInAck ack;
    req.set_vol_name(vol_name);
    req.set_vol_size(vol_size);
    req.set_backup_name(backup_name);
    req.mutable_backup_option()->CopyFrom(backup_option);

    RPC_CALL(backup_stub_, Create, vol_name, req, ack);
    return ack.status();
}

StatusCode InnerRpcClient::list_backup(const std::string& vol_name,
                                  set<std::string>* backup_set) {
    ListBackupInReq req;
    ListBackupInAck ack;
    req.set_vol_name(vol_name);

    RPC_CALL(backup_stub_, List, vol_name, req, ack);
    for (int i = 0; i < ack.backup_name_size(); i++) {
        if (backup_set) {
            backup_set->insert(ack.backup_name(i));
        }
    }
    return ack.status();
}

StatusCode InnerRpcClient::get_backup(const std::string& vol_name,
                                 const std::string& backup_name,
                                 BackupStatus* backup_status) {
    GetBackupInReq req;
    GetBackupInAck ack;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    RPC_CALL(backup_stub_, Get, vol_name, req, ack);
    if (backup_status) {
       *backup_status = ack.backup_status();
    }
    return ack.status();
}

StatusCode InnerRpcClient::delete_backup(const std::string& vol_name,
                                    const std::string& backup_name) {
    DeleteBackupInReq req;
    DeleteBackupInAck ack;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    RPC_CALL(backup_stub_, Delete, vol_name, req, ack);
    return ack.status();
}

StatusCode InnerRpcClient::restore_backup(const std::string& vol_name,
                                     const std::string& backup_name,
                                     const std::string& new_vol_name,
                                     const size_t& new_vol_size,
                                     const std::string& new_block_device,
                                     BlockStore* block_store) {
    RestoreBackupInReq req;
    RestoreBackupInAck ack;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    ClientContext context;
    unique_ptr<ClientReader<RestoreBackupInAck>> reader(backup_stub_->Restore\
                                                       (&context, req));

    int block_dev_fd = open(new_block_device.c_str(), \
                            O_RDWR | O_DIRECT | O_SYNC);
    assert(block_dev_fd != -1);
    char* buf = new char[BACKUP_BLOCK_SIZE];
    assert(buf != nullptr);
    
    while (true) {
        while (reader->Read(&ack) && !ack.blk_over()) {
            uint64_t blk_no = ack.blk_no();
            std::string blk_obj = ack.blk_obj();
            char* blk_data = const_cast<char*>(ack.blk_data().c_str());

            LOG_INFO << "restore blk_no:" << blk_no << " blk_oj:" << blk_obj
                << " blk_data_len:" << ack.blk_data().length();

            if (!blk_obj.empty() && blk_data == nullptr) {
                /*(local)read from block store*/
                int read_ret = block_store->read(blk_obj, buf, \
                        BACKUP_BLOCK_SIZE, 0);
                assert(read_ret == BACKUP_BLOCK_SIZE);
                blk_data = buf;
            }

            if (blk_data) {
                /*write to new block device*/
                int write_ret = pwrite(block_dev_fd, blk_data, BACKUP_BLOCK_SIZE,
                        blk_no * BACKUP_BLOCK_SIZE);
                assert(write_ret == BACKUP_BLOCK_SIZE);
            }
        }
        Status status = reader->Finish();
        if (status.ok()) {
            break;
        } else {
            build_stub(vol_name);
            reader.reset();
            unique_ptr<ClientReader<RestoreBackupInAck>> reader \
                (backup_stub_->Restore(&context, req));
        }
    }

    if (buf) {
        delete [] buf;
    }
    if (block_dev_fd != -1) {
        close(block_dev_fd);
    }

    return StatusCode::sOk; 
}

StatusCode InnerRpcClient::create_replication(const std::string& op_id,
                                            const std::string& rep_id,
                                            const std::string& local_vol,
                                            const list<std::string>& peer_vols,
                                            const RepRole& role) {
    CreateReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_local_volume(local_vol);
    for (std::string v : peer_vols) {
        req.add_peer_volumes(v);
    }
    req.set_rep_uuid(rep_id);
    req.set_role(role);
    ReplicationInnerCommonRes res;

    RPC_CALL(rep_stub_, CreateReplication, local_vol, req, res);
    return res.status();
}

StatusCode InnerRpcClient::enable_replication(const std::string& op_id,
                                              const std::string& vol_id,
                                              const RepRole& role,
                                              const JournalMarker& marker,
                                              const std::string& snap_id) {
    EnableReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    req.mutable_marker()->CopyFrom(marker);
    req.set_snap_id(snap_id);
    ReplicationInnerCommonRes res;
    RPC_CALL(rep_stub_, EnableReplication, vol_id, req, res);
    return res.status();
}

StatusCode InnerRpcClient::disable_replication(const std::string& op_id,
                                               const std::string& vol_id,
                                               const RepRole& role,
                                               const JournalMarker& marker,
                                               const std::string& snap_id) {
    DisableReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    req.mutable_marker()->CopyFrom(marker);
    req.set_snap_id(snap_id);
    ReplicationInnerCommonRes res;
    RPC_CALL(rep_stub_, DisableReplication, vol_id, req, res);
    return res.status();
}

StatusCode InnerRpcClient::failover_replication(const std::string& op_id,
                                                const std::string& vol_id,
                                                const RepRole& role,
                                                const JournalMarker& marker,
                                                const bool& need_sync,
                                                const std::string& snap_id) {
    FailoverReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    req.mutable_marker()->CopyFrom(marker);
    req.set_need_sync(need_sync);
    req.set_snap_id(snap_id);
    ReplicationInnerCommonRes res;
    RPC_CALL(rep_stub_, FailoverReplication, vol_id, req, res);
    return res.status();
}

StatusCode InnerRpcClient::reverse_replication(const std::string& op_id,
                                               const std::string& vol_id,
                                               const RepRole& role) {
    ReverseReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    ReplicationInnerCommonRes res;
    RPC_CALL(rep_stub_, ReverseReplication, vol_id, req, res);
    return res.status();
}

StatusCode InnerRpcClient::delete_replication(const std::string& op_id,
                                              const std::string& vol_id,
                                              const RepRole& role) {
    DeleteReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    ReplicationInnerCommonRes res;
    RPC_CALL(rep_stub_, DeleteReplication, vol_id, req, res);
    return res.status();
}

StatusCode InnerRpcClient::report_checkpoint(const std::string& op_id,
                                             const std::string& vol_id,
                                             const RepRole& role,
                                             bool* is_discard) {
    ReportCheckpointReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    ReportCheckpointRes res;
    RPC_CALL(rep_stub_, ReportCheckpoint, vol_id, req, res);
    if (!res.status()) {
        *is_discard = res.discard_snap();
    }
    return res.status();
}

CtrlRpcClient::CtrlRpcClient(const string& host, const int16_t& port) {
    host_ = host;
    port_ = port;
    std::string addr = rpc_server_address(host, port);
    build_stub(addr);
}

CtrlRpcClient::~CtrlRpcClient() {
}

bool CtrlRpcClient::build_stub(const std::string& addr) {
    channel_ = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    vol_stub_ = VolumeControl::NewStub(channel_);
    snap_stub_ = SnapshotControl::NewStub(channel_);
    backup_stub_ = BackupControl::NewStub(channel_);
    return true;
}

StatusCode CtrlRpcClient::enable_sg(const std::string& volume_id,
                                    const size_t& size,
                                    const std::string& device,
                                map<std::string, std::string>* driver_data) {
    EnableSGReq req;
    req.set_volume_id(volume_id);
    req.set_size(size);
    req.set_device(device);
    EnableSGRes res;

    RPC_CALL(vol_stub_, EnableSG, volume_id, req, res);
    for (auto item : res.driver_data()) {
        driver_data->insert({item.first, item.second});
    }
    return res.status();
}

StatusCode CtrlRpcClient::disable_sg(const std::string& volume_id) {
    DisableSGReq req;
    req.set_volume_id(volume_id);
    ClientContext context;
    DisableSGRes res;

    RPC_CALL(vol_stub_, DisableSG, volume_id, req, res);
    return res.status();
}

StatusCode CtrlRpcClient::get_volume(const std::string& volume_id,
                                     VolumeInfo* volume) {
    GetVolumeReq req;
    // todo
    // req.set_volume_id(volume_id);
    GetVolumeRes res;
    // todo
    // Status status = vol_stub_->GetVolume(&context, req, &res);
    // if (!status.ok()) {
    //    return StatusCode::sInternalError;
    // } else {
    //    if (!res.status()) {
    //        *volume = res.volume();
    //    }
    //    return res.status();
    // }
}

StatusCode CtrlRpcClient::list_volumes(list<VolumeInfo>* volumes) {
    ListVolumesReq req;
    ListVolumesRes res;
    std::string vol_name;
    RPC_CALL(vol_stub_, ListVolumes, vol_name, req, res);
    if (!res.status()) {
        for (auto volume : res.volumes()) {
            volumes->push_back(volume);
        }
    }
    return res.status();
}

StatusCode CtrlRpcClient::list_devices(list<std::string>* devices) {
    ListDevicesReq req;
    ListDevicesRes res;
    std::string volume_id;
    RPC_CALL(vol_stub_, ListDevices, volume_id, req, res);
    if (!res.status()) {
        for (auto device : res.devices()) {
            devices->push_back(device);
        }
    }
    return res.status();
}

StatusCode CtrlRpcClient::create_snapshot(const std::string& vol_name,
                                          const std::string& snap_name) {
    CreateSnapshotReq req;
    CreateSnapshotAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    RPC_CALL(snap_stub_, CreateSnapshot, vol_name, req, ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::list_snapshot(const std::string& vol_name,
                                        set<std::string>* snap_set) {
    ListSnapshotReq req;
    ListSnapshotAck ack;
    req.set_vol_name(vol_name);
    RPC_CALL(snap_stub_, ListSnapshot, vol_name, req, ack);
    for (int i = 0; i < ack.snap_name_size(); i++) {
        snap_set->insert(ack.snap_name(i));
    }
    return ack.header().status();
}

StatusCode CtrlRpcClient::query_snapshot(const std::string& vol_name,
                                         const std::string& snap_name,
                                         SnapStatus* snap_status) {
    QuerySnapshotReq req;
    QuerySnapshotAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    RPC_CALL(snap_stub_, QuerySnapshot, vol_name, req, ack);
    *snap_status = ack.snap_status();
    return ack.header().status();
}

StatusCode CtrlRpcClient::delete_snapshot(const std::string& vol_name,
                                          const std::string& snap_name) {
    DeleteSnapshotReq req;
    DeleteSnapshotAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    RPC_CALL(snap_stub_, DeleteSnapshot, vol_name, req, ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::rollback_snapshot(const std::string& vol_name,
                                            const std::string& snap_name) {
    RollbackSnapshotReq req;
    RollbackSnapshotAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    RPC_CALL(snap_stub_, RollbackSnapshot, vol_name, req, ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::diff_snapshot(const std::string& vol_name,
                                        const std::string& first_snap_name,
                                        const std::string& last_snap_name,
                                        vector<DiffBlocks>* diff) {
    DiffSnapshotReq req;
    DiffSnapshotAck ack;
    req.set_vol_name(vol_name);
    req.set_first_snap_name(first_snap_name);
    req.set_last_snap_name(last_snap_name);
    RPC_CALL(snap_stub_, DiffSnapshot, vol_name, req, ack);
    /*todo: there may be too many diff blocks, should batch and split*/
    int count = ack.diff_blocks_size();
    for (int i = 0; i < count; i++) {
        diff->push_back(ack.diff_blocks(i));
    }
    return ack.header().status();
}

StatusCode CtrlRpcClient::read_snapshot(const std::string& vol_name,
                                        const std::string& snap_name,
                                        const char* buf,
                                        const size_t& len,
                                        const off_t& off) {
    ReadSnapshotReq req;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    req.set_off(off);
    req.set_len(len);
    ReadSnapshotAck ack;
    /*todo: current read snapshot request should send to 
     * machine which hold the block device, how to optimize*/
    RPC_CALL(snap_stub_, ReadSnapshot, vol_name, req, ack);
    memcpy(const_cast<char*>(buf), ack.data().data(), len);
    return ack.header().status();
}

StatusCode CtrlRpcClient::create_volume_from_snap(const std::string& vol_name,
                                               const std::string& snap_name,
                                               const std::string& new_vol,
                                               const std::string& new_blk) {
    CreateVolumeFromSnapReq req;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    req.set_new_vol_name(new_vol);
    req.set_new_blk_device(new_blk);
    CreateVolumeFromSnapAck ack;
    RPC_CALL(snap_stub_, CreateVolumeFromSnap, vol_name, req, ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::query_volume_from_snap(const std::string& new_vol,
                                               VolumeStatus* new_vol_status) {
    QueryVolumeFromSnapReq req;
    req.set_new_vol_name(new_vol);
    QueryVolumeFromSnapAck ack;
    RPC_CALL(snap_stub_, QueryVolumeFromSnap, new_vol, req, ack);
    *new_vol_status = ack.vol_status();
    return ack.header().status();
}

StatusCode CtrlRpcClient::create_backup(const std::string& vol_name,
                                        const size_t& vol_size,
                                        const std::string& backup_name,
                                        const BackupOption& backup_option) {
    CreateBackupReq req;
    req.set_vol_name(vol_name);
    req.set_vol_size(vol_size);
    req.set_backup_name(backup_name);
    req.mutable_backup_option()->CopyFrom(backup_option);
    CreateBackupAck ack;
    RPC_CALL(backup_stub_, CreateBackup, vol_name, req, ack);
    return ack.status();
}

StatusCode CtrlRpcClient::list_backup(const std::string& vol_name,
                                      set<std::string>* backup_set) {
    ListBackupReq req;
    req.set_vol_name(vol_name);
    ListBackupAck ack;
    RPC_CALL(backup_stub_, ListBackup, vol_name, req, ack);
    for (int i = 0; i < ack.backup_name_size(); i++) {
        cout << "ListBackup backup:" << ack.backup_name(i) << endl;
        backup_set->insert(ack.backup_name(i));
    }
    return ack.status();
}

StatusCode CtrlRpcClient::get_backup(const std::string& vol_name,
                                     const std::string& backup_name,
                                     BackupStatus* backup_status) {
    GetBackupReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    GetBackupAck ack;
    RPC_CALL(backup_stub_, GetBackup, vol_name, req, ack);
    *backup_status = ack.backup_status();
    return ack.status();
}

StatusCode CtrlRpcClient::delete_backup(const std::string& vol_name,
                                        const std::string& backup_name) {
    DeleteBackupReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    DeleteBackupAck ack;
    RPC_CALL(backup_stub_, DeleteBackup, vol_name, req, ack);
    return ack.status();
}

StatusCode CtrlRpcClient::restore_backup(const std::string& vol_name,
                                        const std::string& backup_name,
                                        const std::string& new_vol_name,
                                        const size_t& new_vol_size,
                                        const std::string& new_block_device) {
    RestoreBackupReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    req.set_new_vol_name(new_vol_name);
    req.set_new_vol_size(new_vol_size);
    req.set_new_block_device(new_block_device);
    RestoreBackupAck ack;
    RPC_CALL(backup_stub_, RestoreBackup, vol_name, req, ack);
    return ack.status();
}
