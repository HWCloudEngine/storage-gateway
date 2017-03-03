/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    net_receiver.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef NET_RECEIVER_H_
#define NET_RECEIVER_H_
#include "rpc/transfer.grpc.pb.h"
#include "sg_server/replicate/rep_message_handlers.h"

using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using huawei::proto::transfer::DataTransfer;
using grpc::ServerContext;
using grpc::ServerReaderWriter;

class NetReceiver:public DataTransfer::Service{
private:
    RepMsgHandlers& rep_handlers_;
public:
    NetReceiver(RepMsgHandlers& rep_handlers);
    ~NetReceiver();
    grpc::Status transfer(ServerContext* context,
            ServerReaderWriter<TransferResponse,TransferRequest>* stream);

};

#endif

