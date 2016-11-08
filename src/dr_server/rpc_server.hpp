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
#ifndef RPC_SERVER_H_
#define RPC_SERVER_H_
#include <memory>
#include <string>
#include <thread>
#include <grpc++/grpc++.h>
class RpcServer {
private:
    std::unique_ptr<std::thread> thread_;
    std::unique_ptr<grpc::Server> server_;
    grpc::ServerBuilder builder_;
    std::string addr;
public:
    RpcServer(const std::string& host,const int& port,
            const std::shared_ptr<grpc::ServerCredentials>& creds){
        addr.append(host).append(":").append(std::to_string(port));
        AddListening_port(addr,creds);
    }
    RpcServer(){}
    ~RpcServer(){
        if(server_)
            server_->Shutdown();
        if(thread_ && thread_->joinable())
            thread_->join();
    }
    bool register_service(grpc::Service* service){
        builder_.RegisterService(service);
        return true;
    }
    void AddListening_port(const grpc::string& addr,
            const std::shared_ptr<grpc::ServerCredentials>& creds){
        builder_.AddListeningPort(addr,creds);
    }
    bool run(){
        server_ = builder_.BuildAndStart();
        if(!server_)
            return false;
        thread_.reset(new std::thread([&](){
            server_->Wait();
        }));
        return true;
    }
    void join(){
        if(thread_ && thread_->joinable())
            thread_->join();
    }
    void shut_down(){
        if(server_)
            server_->Shutdown();
    }
};
#endif
