/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    backup_proxy.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup entry
*  
*************************************************/
#include "log/log.h"
#include "backup_rpccli.h"

BackupRpcCli::BackupRpcCli() {
}

BackupRpcCli::~BackupRpcCli() {
}

StatusCode BackupRpcCli::CreateBackup(const std::string& vol_name,
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
    grpc::Status status = m_stub->Create(&context, req, &ack);
    return ack.status();
}

StatusCode BackupRpcCli::ListBackup(const std::string& vol_name, std::set<std::string>& backup_set) {
    ClientContext context;
    ListBackupInReq req;
    ListBackupInAck ack;
    req.set_vol_name(vol_name);
    grpc::Status status = m_stub->List(&context, req, &ack);
    for (int i = 0; i < ack.backup_name_size(); i++) {
        backup_set.insert(ack.backup_name(i));
    }
    return ack.status();
}

StatusCode BackupRpcCli::GetBackup(const std::string& vol_name, const std::string& backup_name,
                                   BackupStatus& backup_status) {
    ClientContext context;
    GetBackupInReq req;
    GetBackupInAck ack;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    grpc::Status status = m_stub->Get(&context, req, &ack);
    backup_status = ack.backup_status();
    return ack.status();
}

StatusCode BackupRpcCli::DeleteBackup(const std::string& vol_name, const std::string& backup_name) {
    ClientContext context;
    DeleteBackupInReq req;
    DeleteBackupInAck ack;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    grpc::Status status = m_stub->Delete(&context, req, &ack);
    return ack.status();
}

StatusCode BackupRpcCli::RestoreBackup(const std::string& vol_name, const std::string& backup_name,
                                       const BackupType& backup_type,
                                       const std::string& new_vol_name,
                                       const size_t& new_vol_size,
                                       const std::string& new_block_device,
                                       BlockStore* block_store) {
    ClientContext context;
    RestoreBackupInAck ack;
    RestoreBackupInReq req;
    req.set_vol_name(vol_name);
    req.set_backup_name(backup_name);
    req.set_backup_type(backup_type);
    unique_ptr<ClientReader<RestoreBackupInAck>> reader(m_stub->Restore(&context, req));
    unique_ptr<AccessFile> blk_file;
    Env::instance()->create_access_file(new_block_device, false, true, &blk_file);
    if (blk_file.get() == nullptr) {
        LOG_ERROR << "restore open file failed";
        return StatusCode::sInternalError;
    }
    char* buf = new char[BACKUP_BLOCK_SIZE];
    assert(buf != nullptr);
    while (reader->Read(&ack) && !ack.blk_over()) {
        uint64_t blk_no = ack.blk_no();
        bool blk_zero = ack.blk_zero();
        std::string blk_url = ack.blk_url();
        char* blk_data = (char*)ack.blk_data().c_str();
        size_t blk_data_len = ack.blk_data().length();
        LOG_INFO << "restore blk_no:" << blk_no << " blk_zero:" << blk_zero << " blk_url:" << blk_url
                 << " blk_data_len:" << blk_data_len;
        if (blk_data_len == 0  && !blk_url.empty()) {
            if (!blk_zero) {
                /*(local)read from block store*/
                int read_ret = block_store->read(blk_url, buf, BACKUP_BLOCK_SIZE, 0);
                assert(read_ret == BACKUP_BLOCK_SIZE);
            } else {
                memset(buf, 0, BACKUP_BLOCK_SIZE);
            }
            blk_data = buf;
        }
        if (blk_data) {
            /*write to new block device*/
            ssize_t write_ret = blk_file->write(blk_data, BACKUP_BLOCK_SIZE,
                    blk_no * BACKUP_BLOCK_SIZE);
            assert(write_ret == BACKUP_BLOCK_SIZE);
        }
    }
    Status status = reader->Finish();
    if (buf) {
        delete [] buf;
    }
    return status.ok() ? StatusCode::sOk : StatusCode::sInternalError;
}

void BackupRpcCli::init(std::shared_ptr<Channel> channel){
    m_stub.reset(new BackupInnerControl::Stub(channel));
}

