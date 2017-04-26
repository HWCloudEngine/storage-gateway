/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
* File name:    backup_msg_handler.cc
* Author:
* Date:         2016/11/03
* Version:      1.0
* Description:  remmote backup request dispatch
*
***********************************************/
#include <string>
#include "rpc/transfer.pb.h"
#include "backup_msg_handler.h"

using huawei::proto::transfer::MessageType;

BackupMsgHandler::BackupMsgHandler(BackupMgr& backup_mgr)
    : m_backup_mgr(backup_mgr) {
}

BackupMsgHandler::~BackupMsgHandler() {
}

void BackupMsgHandler::dispatch(TransferRequest* req, RpcIoStream* stream) {
    switch (req->type()) {
        case MessageType::REMOTE_BACKUP_CREATE_START:
            handle_remote_create_start(req, stream);
            break;
        case MessageType::REMOTE_BACKUP_UPLOAD_DATA:
            handle_remote_create_upload(req, stream);
            break;
        case MessageType::REMOTE_BACKUP_CREATE_END:
            handle_remote_create_end(req, stream);
            break;
        case MessageType::REMOTE_BACKUP_DELETE:
            handle_remote_delete(req, stream);
            break;
        case MessageType::REMOTE_BACKUP_DOWNLOAD_DATA:
            handle_download(req, stream);
            break;
        default:
            break;
    }
}

StatusCode BackupMsgHandler::handle_remote_create_start(TransferRequest* req,
                             RpcIoStream* stream) {
    RemoteBackupStartReq start_req;
    RemoteBackupStartAck start_ack;

    start_req.ParseFromString(req->data());

    StatusCode ret = m_backup_mgr.handle_remote_create_start(&start_req,
                                                             &start_ack);

    std::string ack_buf;
    start_ack.SerializeToString(&ack_buf);

    TransferResponse res;
    res.set_type(req->type());
    res.set_data(ack_buf.c_str(), ack_buf.length());
    stream->Write(res);
    return ret;
}

StatusCode BackupMsgHandler::handle_remote_create_end(TransferRequest* req,
                                                      RpcIoStream* stream) {
    RemoteBackupEndReq end_req;
    RemoteBackupEndAck end_ack;

    end_req.ParseFromString(req->data());

    StatusCode ret = m_backup_mgr.handle_remote_create_end(&end_req, &end_ack);

    std::string ack_buf;
    end_ack.SerializeToString(&ack_buf);

    TransferResponse res;
    res.set_type(req->type());
    res.set_data(ack_buf.c_str(), ack_buf.length());
    stream->Write(res);
    return ret;
}

StatusCode BackupMsgHandler::handle_remote_create_upload(TransferRequest* req,
                                                         RpcIoStream* stream) {
    UploadDataReq upload_req;
    UploadDataAck upload_ack;
    upload_req.ParseFromString(req->data());

    StatusCode ret = m_backup_mgr.handle_remote_create_upload(&upload_req,
                                                              &upload_ack);
    return ret;
}

StatusCode BackupMsgHandler::handle_remote_delete(TransferRequest* req,
                                                  RpcIoStream* stream) {
    RemoteBackupDeleteReq del_req;
    RemoteBackupDeleteAck del_ack;

    del_req.ParseFromString(req->data());

    StatusCode ret = m_backup_mgr.handle_remote_delete(&del_req, &del_ack);

    std::string ack_buf;
    del_ack.SerializeToString(&ack_buf);

    TransferResponse res;
    res.set_type(req->type());
    res.set_data(ack_buf.c_str(), ack_buf.length());
    stream->Write(res);
    return ret;
}

StatusCode BackupMsgHandler::handle_download(TransferRequest* req,
                                             RpcIoStream* stream) {
    DownloadDataReq down_req;
    DownloadDataAck down_ack;
    down_req.ParseFromString(req->data());

    StatusCode ret = m_backup_mgr.handle_download(&down_req, stream);
    return ret;
}
