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
#include "common/config_parser.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::JournalMarker;

void RunServer() {
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    int port;
    string type;
    if(false == parser->get<string>("journal_meta_storage.type",type)){
        LOG_FATAL << "config parse journal_meta_storage.type error!";
        return;
    }    
    if(false == parser->get<int>("grpc.server_port",port)){
        LOG_FATAL << "config parse grpc_server.port error!";
        return;
    }
    parser.reset();
    DR_ASSERT(type.compare("ceph_s3")==0);
    std::shared_ptr<CephS3Meta> meta(new CephS3Meta());

    WriterServiceImpl writerSer(meta);
    ConsumerServiceImpl consumerSer(meta);
    // TODO: use async streaming service later

    ServerBuilder builder;
    std::string server_address("0.0.0.0:");
    server_address += std::to_string(port);
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&writerSer);
    builder.RegisterService(&consumerSer);

    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if(nullptr == server) {
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
    DRLog::set_log_level(SG_DEBUG);
    // TODO:check and make sure ceph file system is established and mounted
    RunServer();
    return 0;
}
