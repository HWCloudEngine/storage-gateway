/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    net_sender.cc
* Author: 
* Date:         2017/02/24
* Version:      1.0
* Description:
* 
************************************************/
#include "net_sender.h"
#include <string>
using std::string;

void NetSender::init(std::shared_ptr<Channel> channel){
    channel_=(channel);
    stub_= std::move(huawei::proto::transfer::DataTransfer::NewStub(channel_));
}

std::string NetSender::get_printful_state(ClientState state){
    string str;
    switch(state){
        case CLIENT_IDLE:
            str.append("idle");
            break;
        case CLIENT_CONNECTING:
            str.append("connecting");
            break;
        case CLIENT_READY:
            str.append("ready");
            break;
        case CLIENT_FAILURE:
            str.append("transient_failure");
            break;
        case CLIENT_SHUTDOWN:
            str.append("shutdown");
            break;
        default:
            break;
    }
    return str;
}

ClientState NetSender::get_state(bool try_to_connect){
    return channel_->GetState(try_to_connect);
}

bool NetSender::wait_for_state_change(const ClientState& state,
        std::chrono::system_clock::time_point deadline){
    return channel_->WaitForStateChange(state,deadline);
}

grpc_stream_ptr NetSender::create_stream(ClientContext* ctx){
    if(get_state(false) != CLIENT_READY)
        return nullptr;
    return stub_->transfer(ctx);// grpc stream should be created in work thread
}

StatusCode NetSender::send(TransferRequest* req,ClientContext* ctx){

}


