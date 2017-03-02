/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_receiver.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include "net_receiver.h"
#include "log/log.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using huawei::proto::transfer::MessageType;
using huawei::proto::transfer::EncodeType;
using huawei::proto::transfer::ReplicateDataReq;
using huawei::proto::transfer::ReplicateMarkerReq;
using huawei::proto::transfer::ReplicateStartReq;
using huawei::proto::transfer::ReplicateEndReq;

NetReceiver::NetReceiver(RepMsgHandlers& rep_handlers, BackupMsgHandler& backup_handler):
        rep_handlers_(rep_handlers), backup_handler_(backup_handler){
}

NetReceiver::~NetReceiver(){
}

grpc::Status NetReceiver::transfer(ServerContext* context, 
        ServerReaderWriter<TransferResponse,TransferRequest>* stream){
    static int g_stream_id = 0;
    int stream_id = ++g_stream_id;
    LOG_DEBUG << "replicate receiver stream id: " << stream_id;

    TransferRequest req;
    TransferResponse res;
    while(stream->Read(&req)) {
        res.set_id(req.id());
        res.set_encode(req.encode());
        res.set_type(req.type());

        switch(req.type()){
            case MessageType::REPLICATE_DATA:
                rep_handlers_.rep_handle(req);
                break;
            case MessageType::REPLICATE_MARKER:
            case MessageType::REPLICATE_START:
            case MessageType::REPLICATE_END:
            {
                StatusCode ret_code = rep_handlers_.rep_handle(req);
                    res.set_status(ret_code);
                if(!stream->Write(res)){
                    LOG_ERROR << "response of ending task failed:"
                        << req.id();
                    grpc::Status status(grpc::INTERNAL,"write response of Rep request failed!");
                    return status;
                }
                break;
            }
            case MessageType::REMOTE_BACKUP_CREATE_START:
            case MessageType::REMOTE_BACKUP_UPLOAD_DATA:
            case MessageType::REMOTE_BACKUP_CREATE_END:
            case MessageType::REMOTE_BACKUP_DELETE:
            case MessageType::REMOTE_BACKUP_DOWNLOAD_DATA:
                backup_handler_.dispatch(&req, stream);
                break;
            default:
                break;
        }
    }

    return grpc::Status::OK;
}
