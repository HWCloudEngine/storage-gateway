/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rpc_server.hpp
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:
* 
***********************************************/
#include "rpc_server.h"

RpcServer::RpcServer(const std::string& host, const int& port,
                     const std::shared_ptr<grpc::ServerCredentials>& creds) {
    addr.append(host).append(":").append(std::to_string(port));
    AddListening_port(addr, creds);
}

RpcServer::~RpcServer() {
    if (server_)
        server_->Shutdown();
    if (thread_ && thread_->joinable())
        thread_->join();
}

bool RpcServer::register_service(grpc::Service* service) {
    builder_.RegisterService(service);
    return true;
}

void RpcServer::AddListening_port(const grpc::string& addr,
        const std::shared_ptr<grpc::ServerCredentials>& creds) {
    builder_.AddListeningPort(addr, creds);
}

bool RpcServer::run() {
    server_ = builder_.BuildAndStart();
    if (!server_)
        return false;
    thread_.reset(new std::thread([&](){
                server_->Wait();
                }));
    return true;
}

void RpcServer::join() {
    if (thread_ && thread_->joinable())
        thread_->join();
}

void RpcServer::shut_down() {
    if (server_)
        server_->Shutdown();
}
