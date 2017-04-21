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
    while (true) {                                           \
        grpc::Status status = stub->fname(&ctx, req, &rep);  \
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
}

StatusCode InnerRpcClient::create_volume(const std::string& vol,
                                    const std::string& path,
                                    const uint64_t& size,
                                    const VolumeStatus& s) {
    ClientContext context;
    CreateVolumeReq request;
    CreateVolumeRes response;
    request.set_vol_id(vol);
    request.set_path(path);
    request.set_size(size);
    request.set_status(s);
    grpc::Status status = vol_stub_->CreateVolume(&context, request, &response);
    if (status.ok()) {
        return response.status();
    } else {
        LOG_ERROR << "create volume failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

StatusCode InnerRpcClient::update_volume_status(const std::string& vol,
                                                const VolumeStatus& s) {
    ClientContext context;
    UpdateVolumeStatusReq request;
    UpdateVolumeStatusRes response;
    request.set_status(s);
    request.set_vol_id(vol);
    grpc::Status status = vol_stub_->UpdateVolumeStatus(&context, request,
                                                        &response);
    if (status.ok()) {
        return response.status();
    } else {
        LOG_ERROR << "create volume failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

StatusCode InnerRpcClient::get_volume(const std::string& vol,
                                      VolumeInfo* info) {
    ClientContext context;
    GetVolumeReq request;
    GetVolumeRes response;
    request.set_vol_id(vol);
    grpc::Status status = vol_stub_->GetVolume(&context, request, &response);
    if (status.ok()) {
        info->CopyFrom(response.info());
        return response.status();
    } else {
        LOG_ERROR << "get volume failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

StatusCode InnerRpcClient::list_volume(std::list<VolumeInfo>* list) {
    ClientContext context;
    ListVolumeReq request;
    ListVolumeRes response;
    grpc::Status status = vol_stub_->ListVolume(&context, request, &response);
    if (!status.ok()) {
        LOG_ERROR << "list volume failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
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
    ClientContext context;
    DeleteVolumeReq request;
    DeleteVolumeRes response;
    request.set_vol_id(vol);
    grpc::Status status = vol_stub_->DeleteVolume(&context, request, &response);
    if (status.ok()) {
        return response.status();
    } else {
        LOG_ERROR << "delete volume failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

bool InnerRpcClient::get_writable_journals(const std::string& uuid,
                                      const std::string& vol,
                                      const int limit,
                                      list<JournalElement>* journal_list) {
    // Data we are sending to the server.
    GetWriteableJournalsRequest request;
    request.set_uuid(uuid);
    request.set_vol_id(vol);
    request.set_limits(limit);

    // Container for the data we expect from the server.
    GetWriteableJournalsResponse reply;
    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = writer_stub_->GetWriteableJournals(&context, request,
                                                       &reply);

    // Act upon its status.
    RESULT result = reply.result();
    if (status.ok() && (result == DRS_OK)) {
        for (int i = 0; i < reply.journals_size(); i++) {
            journal_list->push_back(reply.journals(i));
        }
        return true;
    } else {
        LOG_ERROR << "rpc error code:" << status.error_code();
        return false;
    }
}

bool InnerRpcClient::seal_journals(const std::string& uuid,
                                   const std::string& vol,
                                   const std::list<std::string>& list_) {
    // Data we are sending to the server.
    SealJournalsRequest request;
    request.set_uuid(uuid);
    request.set_vol_id(vol);
    std::list<std::string>::const_iterator  iter;
    for (iter = list_.begin(); iter != list_.end(); ++iter) {
        request.add_journals(*iter);
    }

    // Container for the data we expect from the server.
    SealJournalsResponse reply;
    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = writer_stub_->SealJournals(&context, request, &reply);

    // Act upon its status.
    RESULT result = reply.result();
    if (status.ok() && (result == DRS_OK)) {
        return true;
    } else {
        LOG_ERROR << "rpc error code:" << status.error_code();
        return false;
    }
}

bool InnerRpcClient::update_producer_marker(const std::string& uuid,
        const std::string& vol, const JournalMarker& marker) {
    UpdateProducerMarkerRequest request;
    request.set_uuid(uuid);
    request.set_vol_id(vol);
    request.mutable_marker()->CopyFrom(marker);

    UpdateProducerMarkerResponse reply;
    ClientContext context;
    Status status = writer_stub_->UpdateProducerMarker(&context, request,
                                                       &reply);
    if (status.ok() && reply.result() == DRS_OK) {
        return true;
    } else {
        LOG_ERROR << "rpc error code:" << status.error_code();
        return false;
    }
}

bool InnerRpcClient::update_multi_producer_markers(const std::string& uuid,
        const map<std::string, JournalMarker>& markers) {
    UpdateMultiProducerMarkersRequest request;
    request.set_uuid(uuid);
    for (auto it = markers.begin(); it != markers.end(); it++) {
        (*request.mutable_markers())[it->first] = it->second;
    }

    UpdateMultiProducerMarkersResponse reply;
    ClientContext context;
    Status status = writer_stub_->UpdateMultiProducerMarkers(&context, request,
                                                             &reply);
    if (status.ok() && reply.result() == DRS_OK) {
        return true;
    } else {
        LOG_ERROR << "rpc error code:" << status.error_code();
        return false;
    }
}

bool InnerRpcClient::get_journal_marker(const std::string& vol_id,
                                        JournalMarker* marker_) {
    GetJournalMarkerRequest request;
    request.set_vol_id(vol_id);
    request.set_uuid(lease_uuid);
    request.set_type(consumer_type);
    GetJournalMarkerResponse reply;
    ClientContext context;

    Status status = replayer_stub_->GetJournalMarker(&context, request, &reply);
    RESULT result = reply.result();
    if (status.ok() && (result == DRS_OK)) {
        marker_->CopyFrom(reply.marker());
        return true;
    } else {
        return false;
    }
}

bool InnerRpcClient::get_journal_list(const std::string& vol_id,
                               const JournalMarker& marker,
                               const int limit,
                               list<JournalElement>* journal_list) {
    GetJournalListRequest request;
    request.set_limit(limit);
    request.set_vol_id(vol_id);
    request.set_uuid(lease_uuid);
    request.set_type(consumer_type);
    if (!marker.IsInitialized()) {
        return false;
    }
    (request.mutable_marker())->CopyFrom(marker);
    GetJournalListResponse reply;
    ClientContext context;
    Status status = replayer_stub_->GetJournalList(&context, request, &reply);
    RESULT result = reply.result();
    if (status.ok() && (result == DRS_OK)) {
        for (int i = 0; i < reply.journals_size(); i++) {
            journal_list->push_back(reply.journals(i));
        }
        return true;
    } else {
        return false;
    }
}

bool InnerRpcClient::update_consumer_marker(const JournalMarker& marker,
                                       const std::string& vol_id) {
    UpdateConsumerMarkerRequest request;
    request.set_uuid(lease_uuid);
    request.set_vol_id(vol_id);
    request.set_type(consumer_type);
    if (!marker.IsInitialized()) {
        return false;
    }

    (request.mutable_marker())->CopyFrom(marker);

    UpdateConsumerMarkerResponse reply;
    ClientContext context;

    Status status = replayer_stub_->UpdateConsumerMarker(&context, request,
                                                         &reply);
    RESULT result = reply.result();
    if (status.ok() && (result == DRS_OK)) {
        return true;
    } else {
        return false;
    }
}

StatusCode InnerRpcClient::create_snapshot(const SnapReqHead& shead,
                                           const std::string& vname,
                                           const std::string& sname) {
    LOG_INFO << "do_create" << " snap_name:" << sname;

    ClientContext context;
    CreateReq ireq;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);

    CreateAck iack;
    Status st = snap_stub_->Create(&context, ireq, &iack);
    if (!st.ok()) {
        LOG_ERROR << "do_create" << " snap_name:" << sname << " failed";
        return iack.header().status();
    }
    LOG_INFO << "do_create" << " snap_name:" << sname << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::delete_snapshot(const SnapReqHead& shead,
                                           const std::string& vname,
                                           const std::string& sname) {
    LOG_INFO << "do_delete snap_name:" << sname;
    /*really tell dr server delete snapshot*/
    ClientContext context;
    DeleteReq ireq;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);

    DeleteAck iack;
    Status st = snap_stub_->Delete(&context, ireq, &iack);
    if (!st.ok()) {
        LOG_ERROR << "do_delete snap_name:" << sname << " failed";
        return iack.header().status();
    }
    LOG_INFO << "do_delete snap_name:" << sname << " ok";
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
    LOG_INFO << "do_update snap_name:" << sname;
    ClientContext context;
    UpdateReq ireq;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_snap_event(sevent);
    UpdateAck iack;
    Status st = snap_stub_->Update(&context, ireq, &iack);

    if (!st.ok()) {
        LOG_INFO << "do_update snap_name:" << sname << " failed";
        return iack.header().status();
    }
    LOG_INFO << "do_update snap_name:" << sname << " ok";
    return StatusCode::sOk;
}

// todo reture cow object
StatusCode InnerRpcClient::cow(const std::string& vname,
                               const std::string& sname,
                               const size_t& blk_no,
                               enum cow_op* op_type, std::string* op_obj) {
    LOG_INFO << "cow snap_name:" << sname;
    ClientContext ctx1;
    Status status;
    CowReq cow_req;
    CowAck cow_ack;
    cow_req.set_vol_name(vname);
    cow_req.set_snap_name(sname);
    cow_req.set_blk_no(blk_no);
    status = snap_stub_->CowOp(&ctx1, cow_req, &cow_ack);
    if (!status.ok()) {
        return cow_ack.header().status();
    }
    LOG_INFO << "cow snap_name:" << sname << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::cow_update(const std::string& vname,
                                     const std::string& sname,
                                     const size_t& blk_no,
                                     const std::string& blk_obj) {
    LOG_INFO << "cow_update snap_name:" << sname;
    ClientContext ctx2;
    CowUpdateReq update_req;
    CowUpdateAck update_ack;
    update_req.set_vol_name(vname);
    update_req.set_snap_name(sname);
    update_req.set_blk_no(blk_no);
    update_req.set_cow_blk_object(blk_obj);
    Status status = snap_stub_->CowUpdate(&ctx2, update_req, &update_ack);
    if (!status.ok()) {
        return update_ack.header().status();
    }
    LOG_INFO << "cow_update snap_name:" << sname << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::read_snapshot(const SnapReqHead& shead,
                                    const std::string& vname,
                                    const std::string& sname,
                                    const off_t&  off, const size_t& len,
                                    vector<ReadBlock>* read_vec) {
    LOG_INFO << "read snap_name:" << sname << " off:" << off << " len:" << len;
    ClientContext ctx0;
    ReadReq ireq;
    ireq.mutable_header()->CopyFrom(shead);
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_off(off);
    ireq.set_len(len);
    ReadAck iack;
    Status st = snap_stub_->Read(&ctx0, ireq, &iack);
    if (!st.ok()) {
        return iack.header().status();
    }
    LOG_INFO << "read snap_name:" << sname << " off:" << off
             << " len:" << len << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::diff_snapshot(const std::string& vname,
                                         const std::string& first_snap,
                                         const std::string& last_snap,
                                         vector<DiffBlocks>* diff_vec) {
    LOG_INFO << "diff_snapshot vname:" << vname
             << " first_snap:" << first_snap
             << " last_snap:"  << last_snap;

    ClientContext context;
    DiffReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_first_snap_name(first_snap);
    ireq.set_last_snap_name(last_snap);
    DiffAck iack;
    Status st = snap_stub_->Diff(&context, ireq, &iack);
    if (!st.ok()) {
        return iack.header().status();
    }

    int diff_blocks_num = iack.diff_blocks_size();
    for (int i = 0; i < diff_blocks_num; i++) {
        DiffBlocks  idiff_blocks = iack.diff_blocks(i);
        diff_vec->push_back(idiff_blocks);
    }

    LOG_INFO << "diff_snapshot vname:" << vname
             << " first_snap:" << first_snap
             << " last_snap:"  << last_snap << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::list_snapshot(const std::string& vname,
                                         const std::string& sname,
                                         vector<std::string>* snap_vec) {
    LOG_INFO << "list_snapshot vname:" << vname;
    ClientContext context;
    ListReq ireq;
    ireq.set_vol_name(vname);
    ListAck iack;
    Status st = snap_stub_->List(&context, ireq, &iack);
    if (!st.ok()) {
        return iack.header().status();
    }
    int snap_num = iack.snap_name_size();
    for (int i = 0; i < snap_num; i++) {
       snap_vec->push_back(iack.snap_name(i));
    }
    LOG_INFO << "list_snapshot vname:" << vname
             << " snap_size:" << iack.snap_name_size() << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::query_snapshot(const std::string& vname,
                                         const std::string& sname,
                                         SnapStatus* status) {
    LOG_INFO << "query_snapshot vname:" << vname << " sname:" << sname;
    ClientContext context;
    QueryReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    QueryAck iack;
    Status st = snap_stub_->Query(&context, ireq, &iack);
    if (!st.ok()) {
        return iack.header().status();
    }
    *status = iack.snap_status();
    LOG_INFO << "query_snapshot vname:" << vname << " sname:" << sname << " ok";
    return StatusCode::sOk;
}

StatusCode InnerRpcClient::create_backup(const std::string& vol_name,
                                    const size_t& vol_size,
                                    const std::string& backup_name,
                                    const BackupOption& backup_option) {
    CreateBackupInReq req;
    req.set_vol_name(vol_name);
    req.set_vol_size(vol_size);
    req.set_backup_name(backup_name);
    req.mutable_backup_option()->CopyFrom(backup_option);

    CreateBackupInAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->Create(&context, req, &ack);
    return ack.status();
}

StatusCode InnerRpcClient::list_backup(const std::string& vol_name,
                                  set<std::string>* backup_set) {
    ListBackupInReq req;
    req.set_vol_name(vol_name);
    ListBackupInAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->List(&context, req, &ack);
    cout << "ListBackup size:" << ack.backup_name_size() << endl;
    for (int i = 0; i < ack.backup_name_size(); i++) {
        cout << "ListBackup backup:" << ack.backup_name(i) << endl;
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
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    GetBackupInAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->Get(&context, req, &ack);
    if (backup_status) {
       *backup_status = ack.backup_status();
    }
    return ack.status();
}

StatusCode InnerRpcClient::delete_backup(const std::string& vol_name,
                                    const std::string& backup_name) {
    DeleteBackupInReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    DeleteBackupInAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->Delete(&context, req, &ack);
    return ack.status();
}

StatusCode InnerRpcClient::restore_backup(const std::string& vol_name,
                                     const std::string& backup_name,
                                     const std::string& new_vol_name,
                                     const size_t& new_vol_size,
                                     const std::string& new_block_device,
                                     BlockStore* block_store) {
    RestoreBackupInReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    RestoreBackupInAck ack;
    ClientContext context;
    unique_ptr<ClientReader<RestoreBackupInAck>> reader(backup_stub_->Restore\
                                                       (&context, req));

    int block_dev_fd = open(new_block_device.c_str(), \
                            O_RDWR | O_DIRECT | O_SYNC);
    assert(block_dev_fd != -1);
    char* buf = new char[BACKUP_BLOCK_SIZE];
    assert(buf != nullptr);

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
    if (buf) {
        delete [] buf;
    }
    if (block_dev_fd != -1) {
        close(block_dev_fd);
    }
    return status.ok() ? StatusCode::sOk : StatusCode::sInternalError;
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
    ClientContext context;
    ReplicationInnerCommonRes res;
    grpc::Status status = rep_stub_->CreateReplication(&context, req, &res);
    if (status.ok()) {
        return res.status();
    } else {
        LOG_ERROR << "create replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
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
    ClientContext context;
    ReplicationInnerCommonRes res;
    grpc::Status status = rep_stub_->EnableReplication(&context, req, &res);
    if (status.ok()) {
        return res.status();
    } else {
        LOG_ERROR << "enable replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
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
    ClientContext context;
    ReplicationInnerCommonRes res;
    grpc::Status status = rep_stub_->DisableReplication(&context, req, &res);
    if (status.ok()) {
        return res.status();
    } else {
        LOG_ERROR << "disable replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
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

    ClientContext context;
    ReplicationInnerCommonRes res;
    grpc::Status status = rep_stub_->FailoverReplication(&context, req, &res);
    if (status.ok()) {
        return res.status();
    } else {
        LOG_ERROR << "failover replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

StatusCode InnerRpcClient::reverse_replication(const std::string& op_id,
                                               const std::string& vol_id,
                                               const RepRole& role) {
    ReverseReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    ClientContext context;
    ReplicationInnerCommonRes res;
    grpc::Status status = rep_stub_->ReverseReplication(&context, req, &res);
    if (status.ok()) {
        return res.status();
    } else {
        LOG_ERROR << "reverse replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

StatusCode InnerRpcClient::delete_replication(const std::string& op_id,
                                              const std::string& vol_id,
                                              const RepRole& role) {
    DeleteReplicationInnerReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    ClientContext context;
    ReplicationInnerCommonRes res;
    grpc::Status status = rep_stub_->DeleteReplication(&context, req, &res);
    if (status.ok()) {
        return res.status();
    } else {
        LOG_ERROR << "delete replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
}

StatusCode InnerRpcClient::report_checkpoint(const std::string& op_id,
                                             const std::string& vol_id,
                                             const RepRole& role,
                                             bool* is_discard) {
    ReportCheckpointReq req;
    req.set_operate_id(op_id);
    req.set_vol_id(vol_id);
    req.set_role(role);
    ClientContext context;
    ReportCheckpointRes res;
    grpc::Status status = rep_stub_->ReportCheckpoint(&context, req, &res);
    if (status.ok()) {
        if (!res.status()) {
            *is_discard = res.discard_snap();
        }
        return res.status();
    } else {
        LOG_ERROR << "failover replication failed:"
            << status.error_message() << ",code:" << status.error_code();
        return StatusCode::sInternalError;
    }
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
    ClientContext context;
    EnableSGRes res;

    Status status = vol_stub_->EnableSG(&context, req, &res);
    if (!status.ok()) {
        return StatusCode::sInternalError;
    } else {
        for (auto item : res.driver_data()) {
            driver_data->insert({item.first, item.second});
        }
        return res.status();
    }
}

StatusCode CtrlRpcClient::disable_sg(const std::string& volume_id) {
    DisableSGReq req;
    req.set_volume_id(volume_id);
    ClientContext context;
    DisableSGRes res;

    Status status = vol_stub_->DisableSG(&context, req, &res);
    if (!status.ok()) {
        return StatusCode::sInternalError;
    } else {
        return res.status();
    }
}

StatusCode CtrlRpcClient::get_volume(const std::string& volume_id,
                                     VolumeInfo* volume) {
    GetVolumeReq req;
    // todo
    // req.set_volume_id(volume_id);
    ClientContext context;
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
    ClientContext context;
    ListVolumesRes res;

    Status status = vol_stub_->ListVolumes(&context, req, &res);
    if (!status.ok()) {
        return StatusCode::sInternalError;
    } else {
        if (!res.status()) {
            for (auto volume : res.volumes()) {
                volumes->push_back(volume);
            }
        }
        return res.status();
    }
}

StatusCode CtrlRpcClient::list_devices(list<std::string>* devices) {
    ListDevicesReq req;
    ClientContext context;
    ListDevicesRes res;

    Status status = vol_stub_->ListDevices(&context, req, &res);
    if (!status.ok()) {
        return StatusCode::sInternalError;
    } else {
        if (!res.status()) {
            for (auto device : res.devices()) {
                devices->push_back(device);
            }
        }
        return res.status();
    }
}

StatusCode CtrlRpcClient::create_snapshot(const std::string& vol_name,
                                          const std::string& snap_name) {
    CreateSnapshotReq req;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    CreateSnapshotAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->CreateSnapshot(&context, req, &ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::list_snapshot(const std::string& vol_name,
                                        set<std::string>* snap_set) {
    ListSnapshotReq req;
    req.set_vol_name(vol_name);
    ListSnapshotAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->ListSnapshot(&context, req, &ack);
    for (int i = 0; i < ack.snap_name_size(); i++) {
        snap_set->insert(ack.snap_name(i));
    }
    return ack.header().status();
}

StatusCode CtrlRpcClient::query_snapshot(const std::string& vol_name,
                                         const std::string& snap_name,
                                         SnapStatus* snap_status) {
    QuerySnapshotReq req;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    QuerySnapshotAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->QuerySnapshot(&context, req, &ack);
    *snap_status = ack.snap_status();
    return ack.header().status();
}

StatusCode CtrlRpcClient::delete_snapshot(const std::string& vol_name,
                                          const std::string& snap_name) {
    DeleteSnapshotReq req;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    DeleteSnapshotAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->DeleteSnapshot(&context, req, &ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::rollback_snapshot(const std::string& vol_name,
                                            const std::string& snap_name) {
    RollbackSnapshotReq req;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    RollbackSnapshotAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->RollbackSnapshot(&context, req, &ack);
    return ack.header().status();
}

StatusCode CtrlRpcClient::diff_snapshot(const std::string& vol_name,
                                        const std::string& first_snap_name,
                                        const std::string& last_snap_name,
                                        vector<DiffBlocks>* diff) {
    DiffSnapshotReq req;
    req.set_vol_name(vol_name);
    req.set_first_snap_name(first_snap_name);
    req.set_last_snap_name(last_snap_name);
    DiffSnapshotAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->DiffSnapshot(&context, req, &ack);

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
    ClientContext context;
    /*todo: current read snapshot request should send to 
     * machine which hold the block device, how to optimize*/
    grpc::Status status = snap_stub_->ReadSnapshot(&context, req, &ack);
    if (!status.ok()) {
        return ack.header().status();
    }
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
    ClientContext context;

    grpc::Status status = snap_stub_->CreateVolumeFromSnap(&context, req, &ack);
    if (!status.ok()) {
        return ack.header().status();
    }
    return ack.header().status();
}

StatusCode CtrlRpcClient::query_volume_from_snap(const std::string& new_vol,
                                               VolumeStatus* new_vol_status) {
    QueryVolumeFromSnapReq req;
    req.set_new_vol_name(new_vol);
    QueryVolumeFromSnapAck ack;
    ClientContext context;
    grpc::Status status = snap_stub_->QueryVolumeFromSnap(&context, req, &ack);
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
    ClientContext context;
    grpc::Status status = backup_stub_->CreateBackup(&context, req, &ack);
    return ack.status();
}

StatusCode CtrlRpcClient::list_backup(const std::string& vol_name,
                                      set<std::string>* backup_set) {
    ListBackupReq req;
    req.set_vol_name(vol_name);
    ListBackupAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->ListBackup(&context, req, &ack);
    cout << "ListBackup size:" << ack.backup_name_size() << endl;
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
    ClientContext context;
    grpc::Status status = backup_stub_->GetBackup(&context, req, &ack);
    *backup_status = ack.backup_status();
    return ack.status();
}

StatusCode CtrlRpcClient::delete_backup(const std::string& vol_name,
                                        const std::string& backup_name) {
    DeleteBackupReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    DeleteBackupAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->DeleteBackup(&context, req, &ack);
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
    ClientContext context;
    grpc::Status status = backup_stub_->RestoreBackup(&context, req, &ack);
    return ack.status();
}
