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
#include <thread>
#include <vector>
#include <sys/types.h>
#include <ifaddrs.h>
#include <sys/socket.h> //getnameinfo
#include <netdb.h> //NI_MAXHOST
#include "log/log.h"
#include "ceph_s3_meta.h"
#include "common/config_parser.h"
#include "gc_task.h"
#include "rpc_server.hpp"
#include "writer_service.h"
#include "consumer_service.h"
#include "replicate/rep_receiver.h"
#include "replicate/replicate.h"
#include "../snapshot/snapshot_mgr.h"
#define DEFAULT_META_SERVER_PORT 50051
#define DEFAULT_REPLICATE_PORT 50061

bool get_local_ip(string& ip,const char* name="eth0",const int type=AF_INET){
    struct ifaddrs *ifaddr, *ifa;
    int family, s, n;
    char host[NI_MAXHOST];
    bool flag = false;
    
    if(getifaddrs(&ifaddr) == -1){
        LOG_ERROR << "getifaddrs failed!";
        return false;
    }

    for(ifa = ifaddr, n = 0; ifa != NULL; ifa = ifa->ifa_next, n++){
        if(ifa->ifa_addr == NULL)
            continue;
        family = ifa->ifa_addr->sa_family;

        if(family != type)
            continue;
        if(0 != strcmp(ifa->ifa_name,name))
            continue;
        s = getnameinfo(ifa->ifa_addr,
            (family == AF_INET) ? sizeof(struct sockaddr_in) :
                sizeof(struct sockaddr_in6),
            host, NI_MAXHOST,
            NULL, 0, NI_NUMERICHOST);
        if(s != 0) {
            LOG_ERROR << "getnameinfo() failed: " << gai_strerror(s);
            break;
        }
        flag = true;
        ip.clear();
        ip.append(host);
        LOG_INFO << "get local ip address: " << host;
    }

    freeifaddrs(ifaddr);
    return flag;
}

int main(int argc, char** argv) {
    std::string file="drserver.log";
    DRLog::log_init(file);
    DRLog::set_log_level(SG_DEBUG);
    std::shared_ptr<CephS3Meta> meta(new CephS3Meta());
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    
    string local_lo("127.0.0.1");
    string ip1,ip2;
    int port1,port2;
    string addr;
    string mount_path;
    ip1 = parser->get_default<string>("meta_server.ip",local_lo);  
    if(! parser->get<string>("replicate.local_ip",ip2)){
        if(!get_local_ip(ip2)){
            LOG_FATAL << "config parse replicate.local_ip error!";
            std::cerr << "config parse replicate.local_ip error!" << std::endl;
            return -1;
        }
    }
    port1 = parser->get_default<int>("meta_server.port",DEFAULT_META_SERVER_PORT);
    port2 = parser->get_default<int>("replicate.port",DEFAULT_REPLICATE_PORT);
    if(false == parser->get<string>("replicate.remote_ip",addr)){
        LOG_FATAL << "config parse replicate.remote_ip error!";
        std::cerr << "config parse replicate.remote_ip error!" << std::endl;
        return -1;
    }
    string type;
    if(false == parser->get<string>("journal_storage.type",type)){
        LOG_FATAL << "config parse journal_storage.type error!";
        std::cerr << "config parse journal_storage.type error!" << std::endl;
        return -1;
    }
    if(type.compare("ceph_fs") == 0){        
        if(false == parser->get<string>("ceph_fs.mount_point",mount_path)){
            LOG_FATAL << "config parse ceph_fs.mount_point error!";
            std::cerr << "config parse ceph_fs.mount_point error!" << std::endl;
            return -1;
        }
    }
    else{
        LOG_FATAL << "journal storage type[" << type << "] is invalid!";
        std::cerr << "journal storage type[" << type << "] is invalid!" << std::endl;
        return -1;
    }
    // init meta server
    RpcServer metaServer(ip1,port1,grpc::InsecureServerCredentials());
    WriterServiceImpl writerSer(meta);
    ConsumerServiceImpl consumerSer(meta);
    SnapshotMgr snapshot_mgr;
    metaServer.register_service(&writerSer);
    metaServer.register_service(&consumerSer);
    metaServer.register_service(&snapshot_mgr);
    LOG_INFO << "meta server listening on " << ip1 << ":" << port1;
    if(!metaServer.run()){
        LOG_FATAL << "start meta server failed!";
        std::cerr << "start meta server failed!" << std::endl;
        return -1;
    }
    
    // init replicate receiver
    RpcServer repServer(ip2,port2,grpc::InsecureServerCredentials());
    RepReceiver repReceiver(meta,mount_path);
    repServer.register_service(&repReceiver);
    LOG_INFO << "replicate server listening on " << ip2 << ":" << port2;
    if(!repServer.run()){
        LOG_FATAL << "start replicate server failed!";
        std::cerr << "start replicate server failed!" << std::endl;
        return -1;
    }
    // init replicate client
    addr.append(":").append(std::to_string(port2));
    Replicate replicate(meta,mount_path,addr,grpc::InsecureChannelCredentials());
    // init gc thread
    GCTask::instance().init(meta);

    // config volumes
    // TODO: this config is for test only, drserver may read volume configs from its meta later
    std::vector<string> vols;
    if(parser->get_array<string>("volumes.primary",vols)){
        for(auto vol:vols){
            replicate.add_volume(vol);
            GCTask::instance().add_volume(vol);
        }
    }
    parser.reset();
    metaServer.join();
    repServer.join();
    return 0;
}
