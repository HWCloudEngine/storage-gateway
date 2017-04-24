/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
* File name:    backup_mgr.h
* Author:
* Date:         2016/11/03
* Version:      1.0
* Description:  remmote backup request dispatch
*
***********************************************/
#ifndef SRC_SG_SERVER_BACKUP_BACKUP_MSG_HANDLER_H_
#define SRC_SG_SERVER_BACKUP_BACKUP_MSG_HANDLER_H_

#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/transfer.pb.h"
#include "backup_mgr.h"
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;

using RpcIoStream = ServerReaderWriter<TransferResponse, TransferRequest>;

class BackupMsgHandler {
 public:
    explicit BackupMsgHandler(BackupMgr& backup_mgr);
    ~BackupMsgHandler();

    void dispatch(TransferRequest* req, RpcIoStream* stream);

 private:
    StatusCode handle_remote_create_start(TransferRequest* req,
                                          RpcIoStream* stream);
    StatusCode handle_remote_create_end(TransferRequest* req,
                                        RpcIoStream* stream);
    StatusCode handle_remote_create_upload(TransferRequest* req,
                                           RpcIoStream* stream);
    StatusCode handle_remote_delete(TransferRequest* req,
                                    RpcIoStream* stream);
    StatusCode handle_download(TransferRequest* req,
                               RpcIoStream* stream);

 private:
    BackupMgr& m_backup_mgr;
};

#endif  // SRC_SG_SERVER_BACKUP_BACKUP_MSG_HANDLER_H_
