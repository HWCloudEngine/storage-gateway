/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:	rpc_server.cc
* Author: 
* Date:			2016/07/06
* Version: 		1.0
* Description:
* 
**********************************************/
#include <iostream>
#include <memory>
#include <string>
#include "writer_service.h"
#include "consumer_service.h"
#include "ceph_s3_meta.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::JournalMarker;
 // TODO: move the const params into config file
const char access_key[] = "7NNRDDYU2QPPCICMW6U8";
const char secret_key[] = "3Ui5pO0UfvSfwT5e5JYXfVIptMHLCDgVpBzkVnb2";
const char host[] = "ceph-node1:7480"; // port number is necessary if not using default 80/443
const char my_bucket[] = "journals_bucket";

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    std::unique_ptr<CephS3Meta> meta(new CephS3Meta());
    RESULT res = (meta.get())->init(access_key,secret_key,host,my_bucket);
    if(res != DRS_OK) {
        std::cerr << "ceph meta init failed!" << std::endl;
        return;
        // TODO
    }
    WriterServiceImpl writerSer(meta.get());
    ConsumerServiceImpl consumerSer(meta.get());
    //TODO: use async streaming service later

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&writerSer);
    builder.RegisterService(&consumerSer);

    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    RunServer();
    return 0;
}
