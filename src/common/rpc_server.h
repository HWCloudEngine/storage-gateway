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
#ifndef SRC_COMMON_RPC_SERVER_H_
#define SRC_COMMON_RPC_SERVER_H_
#include <grpc++/grpc++.h>
#include <memory>
#include <string>
#include <thread>

class RpcServer {
 private:
    std::unique_ptr<std::thread> thread_;
    std::unique_ptr<grpc::Server> server_;
    grpc::ServerBuilder builder_;
    std::string addr;

 public:
    RpcServer(const std::string& host, const int& port,
              const std::shared_ptr<grpc::ServerCredentials>& creds);
    RpcServer() {}
    ~RpcServer();
    bool register_service(grpc::Service* service);
    void AddListening_port(const grpc::string& addr,
            const std::shared_ptr<grpc::ServerCredentials>& creds);

    bool run();
    void join();
    void shut_down();
};
#endif  // SRC_COMMON_RPC_SERVER_H_
