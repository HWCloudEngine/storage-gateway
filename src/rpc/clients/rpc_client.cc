/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   rpc_client.h 
*  Author: 
*  Date:         2017/06/28
*  Version:      1.0
*  Description:  handle writer io
*
*************************************************/
#include "rpc_client.h"

//TODO the other client stub
std::unique_ptr<Writer::Stub> WriterClient::stub_;

void rpc_init(){
    std::string meta_rpc_addr = rpc_address(g_option.meta_server_ip, g_option.meta_server_port);
    WriterClient::init(grpc::CreateChannel(meta_rpc_addr,
                    grpc::InsecureChannelCredentials()));
    //TODO the other client init(snapshot,backup,replayer...)
}

RpcClient::RpcClient(){
    rpc_init();
    register_func();
}

RpcClient& RpcClient::instance(){
    static RpcClient rpc_client;
    return rpc_client;
}

void RpcClient::register_func()
{
    //WriterClient
    GetWriteableJournals = make_decorator(&WriterClient::GetWriteableJournals);
    SealJournals = make_decorator(&WriterClient::SealJournals);
    update_producer_marker = make_decorator(&WriterClient::update_producer_marker);
    update_multi_producer_markers = make_decorator(&WriterClient::update_multi_producer_markers);

    //TODO the other client register(snapshot,backup,replayer...)
}


