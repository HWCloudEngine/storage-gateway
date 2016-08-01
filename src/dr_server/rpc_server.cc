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
const char config_file[] = "/etc/storage-gateway/config.ini";

void RunServer() {
    std::unique_ptr<ConfigParser> parser(new ConfigParser(config_file));
    string access_key;
    string secret_key;
    string host;
    string bucket_name;
    string mount_path;
    int port;
    if(false == parser->get<string>("ceph_s3.access_key",access_key)){
        LOG_FATAL << "config parse ceph_s3.access_key error!";
        return;
    }
    if(false == parser->get<string>("ceph_s3.secret_key",secret_key)){
        LOG_FATAL << "config parse ceph_s3.secret_key error!";
        return;
    }
    if(false == parser->get<string>("ceph_s3.host",host)){ // port number is necessary if not using default 80/443
        LOG_FATAL << "config parse ceph_s3.host error!";
        return;
    }
    if(false == parser->get<string>("ceph_s3.bucket",bucket_name)){
        LOG_FATAL << "config parse ceph_s3.bucket error!";
        return;
    }
    if(false == parser->get<string>("ceph_fs.mount_point",mount_path)){
        LOG_FATAL << "config parse ceph_fs.mount_point error!";
        return;
    }
    if(false == parser->get<int>("grpc_server.port",port)){
        LOG_FATAL << "config parse grpc_server.port error!";
        return;
    }
    parser.reset();
    std::unique_ptr<CephS3Meta> meta(new CephS3Meta());
    RESULT res = (meta.get())->init(access_key.c_str(),secret_key.c_str(),
        host.c_str(),bucket_name.c_str(),mount_path.c_str());
    if(res != DRS_OK) {
        LOG_FATAL << "ceph meta init failed!";
        return;
    }
    WriterServiceImpl writerSer(meta.get());
    ConsumerServiceImpl consumerSer(meta.get());
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
    DRLog::set_log_level(debug);
    // TODO:check and make sure ceph file system is established and mounted
    RunServer();
    return 0;
}
