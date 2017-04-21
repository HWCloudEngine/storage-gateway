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

RpcClient::RpcClient(const std::string& host, const int16_t& port) {
    host_ = host;
    port_ = port;
    std::string addr = rpc_server_address(host, port);
    channel_ = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());

    vol_stub_ = VolumeInnerControl::NewStub(channel_);
    // snap_stub_ = SnapshotInnerControl::NewStub(channel_);
    backup_stub_ = BackupInnerControl::NewStub(channel_);
    rep_stub_ = ReplicateInnerControl::NewStub(channel_);
}

RpcClient::~RpcClient() {
}

StatusCode RpcClient::create_volume(const std::string& vol,
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

StatusCode RpcClient::update_volume_status(const std::string& vol,
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

StatusCode RpcClient::get_volume(const std::string& vol, VolumeInfo* info) {
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

StatusCode RpcClient::list_volume(std::list<VolumeInfo>* list) {
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

StatusCode RpcClient::delete_volume(const std::string& vol) {
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

StatusCode RpcClient::create_backup(const std::string& vol_name,
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

StatusCode RpcClient::list_backup(const std::string& vol_name,
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

StatusCode RpcClient::get_backup(const std::string& vol_name,
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

StatusCode RpcClient::delete_backup(const std::string& vol_name,
                                    const std::string& backup_name) {
    DeleteBackupInReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    DeleteBackupInAck ack;
    ClientContext context;
    grpc::Status status = backup_stub_->Delete(&context, req, &ack);
    return ack.status();
}

StatusCode RpcClient::restore_backup(const std::string& vol_name,
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

