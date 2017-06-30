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
#ifndef SRC_SG_CLIENT_BACKUP_BACKUP_RPCCLI_H_
#define SRC_SG_CLIENT_BACKUP_BACKUP_RPCCLI_H_
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <fstream>
#include <memory>
#include <set>
#include <vector>
#include <grpc++/grpc++.h>
#include "rpc/backup.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "common/define.h"
#include "common/block_store.h"
#include "common/env_posix.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;
using huawei::proto::StatusCode;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;
using huawei::proto::BackupStatus;
using huawei::proto::BackupOption;
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

/*backup control rpc client*/
class BackupRpcCli {

 public:
    explicit BackupRpcCli(std::shared_ptr<Channel> channel);
    BackupRpcCli(const BackupRpcCli& other) = delete;
    BackupRpcCli& operator=(const BackupRpcCli& other) = delete;
    ~BackupRpcCli();

    StatusCode CreateBackup(const std::string& vol_name, const size_t& vol_size,
                            const std::string& backup_name,
                            const BackupOption& backup_option);
    StatusCode ListBackup(const std::string& vol_name, std::set<std::string>& backup_set);
    StatusCode GetBackup(const std::string& vol_name, const std::string& backup_name,
                         BackupStatus& backup_status);
    StatusCode DeleteBackup(const std::string& vol_name, const std::string& backup_name);
    StatusCode RestoreBackup(const std::string& vol_name,
                             const std::string& backup_name,
                             const BackupType& backup_type,
                             const std::string& new_vol_name,
                             const size_t& new_vol_size,
                             const std::string& new_block_device,
                             BlockStore* block_store);

 private:
    std::unique_ptr<BackupInnerControl::Stub> m_stub;
};

#endif  // SRC_SG_CLIENT_BACKUP_BACKUP_RPCCLI_H_
