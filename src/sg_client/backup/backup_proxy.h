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
#ifndef SRC_SG_CLIENT_BACKUP_BACKUP_PROXY_H_
#define SRC_SG_CLIENT_BACKUP_BACKUP_PROXY_H_
#include <string>
#include <memory>
#include "common/block_store.h"
#include "common/volume_attr.h"
#include "rpc/common.pb.h"
#include "rpc/backup_control.pb.h"
#include "rpc/backup_control.grpc.pb.h"
#include "rpc/clients/rpc_client.h"
#include "backup_decorator.h"

using huawei::proto::StatusCode;
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

class BackupProxy {
 public:
    BackupProxy(VolumeAttr& vol_attr, shared_ptr<BackupDecorator> backup_decorator);
    virtual ~BackupProxy();

    /*snapshot common operation*/
    StatusCode create_backup(const CreateBackupReq* req, CreateBackupAck* ack);
    StatusCode delete_backup(const DeleteBackupReq* req, DeleteBackupAck* ack);
    StatusCode restore_backup(const RestoreBackupReq* req, RestoreBackupAck* ack);
    StatusCode list_backup(const ListBackupReq* req, ListBackupAck* ack);
    StatusCode get_backup(const GetBackupReq* req, GetBackupAck* ack);

 private:
    bool is_backup_exist(const std::string& vol, const std::string& bname);

 private:
    /*volume basic*/
    VolumeAttr& m_vol_attr;
    shared_ptr<BackupDecorator> m_backup_decorator;
    shared_ptr<BlockStore> m_block_store;
};

#endif  // SRC_SG_CLIENT_BACKUP_BACKUP_PROXY_H_
