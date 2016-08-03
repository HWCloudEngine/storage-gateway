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
#include "log/log.h"

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
const char ceph_mount_path[] = "/mnt/cephfs";

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    std::unique_ptr<CephS3Meta> meta(new CephS3Meta());
    RESULT res = (meta.get())->init(access_key,secret_key,host,my_bucket,ceph_mount_path);
    if(res != DRS_OK) {
        LOG_ERROR << "ceph meta init failed!";
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
    if(nullptr == server) {
        std::cout << "init server failed:BuildAndStart()!" << std::endl;
        LOG_FATAL << "band " << server_address <<" failed!";
        return;
    }
    LOG_DEBUG << "Server listening on " << server_address;
    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    std::string file="drserver.log";
    DRLog::log_init(file);
    DRLog::set_log_level(debug);
    // TODO:check and make sure ceph file system is established and mounted
    RunServer();
    return 0;
}
