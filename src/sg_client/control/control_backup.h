/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    control_backup.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup control interface export to highlevel control layer 
*
*************************************************/
#ifndef SRC_SG_CLIENT_CONTROL_CONTROL_BACKUP_H_
#define SRC_SG_CLIENT_CONTROL_CONTROL_BACKUP_H_
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/backup_control.pb.h"
#include "rpc/backup_control.grpc.pb.h"
#include "log/log.h"
#include "../backup/backup_proxy.h"
#include "../volume.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::control::BackupControl;

using huawei::proto::control::CreateBackupReq;
using huawei::proto::control::CreateBackupAck;
using huawei::proto::control::DeleteBackupReq;
using huawei::proto::control::DeleteBackupAck;
using huawei::proto::control::RestoreBackupReq;
using huawei::proto::control::RestoreBackupAck;
using huawei::proto::control::GetBackupReq;
using huawei::proto::control::GetBackupAck;
using huawei::proto::control::ListBackupReq;
using huawei::proto::control::ListBackupAck;

using namespace std;

class BackupControlImpl final: public BackupControl::Service {
 public:
    explicit BackupControlImpl(map<string, shared_ptr<Volume>>& volumes)
        :m_volumes(volumes) {
    }

    virtual ~BackupControlImpl() {
    }

    Status CreateBackup(ServerContext* context,
                        const CreateBackupReq* req,
                        CreateBackupAck* ack) override;

    Status DeleteBackup(ServerContext* context,
                        const DeleteBackupReq* req,
                        DeleteBackupAck* ack) override;

    Status RestoreBackup(ServerContext* context,
                         const RestoreBackupReq* req,
                         RestoreBackupAck* ack) override;

    Status GetBackup(ServerContext* contex,
                       const GetBackupReq* req,
                       GetBackupAck* ack) override;

    Status ListBackup(ServerContext* contex,
                      const ListBackupReq* req,
                      ListBackupAck* ack) override;

 private:
    /*each volume own corresponding backup proxy*/
    shared_ptr<BackupProxy> get_vol_backup_proxy(const string& vol_name);

 private:
    map<string, shared_ptr<Volume>>& m_volumes;
};

#endif  // SRC_SG_CLIENT_CONTROL_CONTROL_BACKUP_H_
