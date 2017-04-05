#ifndef BACKUP_MSG_HANDLER_H
#define BACKUP_MSG_HANDLER_H
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/transfer.pb.h"
#include "backup_mgr.h"
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;

using RpcIoStream = ServerReaderWriter<TransferResponse, TransferRequest>;

class BackupMsgHandler
{
public:
    BackupMsgHandler(BackupMgr& backup_mgr);
    ~BackupMsgHandler();
    
    void dispatch(TransferRequest* req, RpcIoStream* stream);

private:
    StatusCode handle_remote_create_start(TransferRequest* req, RpcIoStream* stream);
    StatusCode handle_remote_create_end(TransferRequest* req, RpcIoStream* stream);
    StatusCode handle_remote_create_upload(TransferRequest* req, RpcIoStream* stream);
    StatusCode handle_remote_delete(TransferRequest* req, RpcIoStream* stream);
    StatusCode handle_download(TransferRequest* req, RpcIoStream* stream);

private:
    BackupMgr& m_backup_mgr;
};

#endif
