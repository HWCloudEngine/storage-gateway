/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    net_sender.h
* Author: 
* Date:         2017/02/24
* Version:      1.0
* Description:
* 
************************************************/
#ifndef NET_SENDER_H_
#define NET_SENDER_H_
#include <grpc++/grpc++.h>
#include "rpc/transfer.grpc.pb.h"
using grpc::Channel;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using huawei::proto::StatusCode;

typedef grpc_connectivity_state ClientState;
#define CLIENT_IDLE         GRPC_CHANNEL_IDLE
#define CLIENT_CONNECTING   GRPC_CHANNEL_CONNECTING
#define CLIENT_READY        GRPC_CHANNEL_READY
#define CLIENT_FAILURE      GRPC_CHANNEL_TRANSIENT_FAILURE
#define CLIENT_SHUTDOWN     GRPC_CHANNEL_SHUTDOWN


typedef std::shared_ptr<ClientReaderWriter<TransferRequest,TransferResponse>> grpc_stream_ptr;

class NetSender{
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<huawei::proto::transfer::DataTransfer::Stub> stub_;
public:
    static NetSender& instance(){
        static NetSender nt;
        return nt;
    }
    NetSender(NetSender&) = delete;
    NetSender& operator=(NetSender const&) = delete;

    void init(std::shared_ptr<Channel> channel);
    // get rpc state
    ClientState get_state(bool try_to_connect);
    bool wait_for_state_change(const ClientState& state,
            std::chrono::system_clock::time_point deadline);
    std::string get_printful_state(ClientState state);

    // create a grpc stream; the stream should be used(write/read) in the same
    // thread in which the stream is created
    grpc_stream_ptr create_stream(ClientContext* ctx);
    StatusCode send(TransferRequest* req,ClientContext* ctx);

private:
    NetSender(){};
    ~NetSender(){};
};
#endif
