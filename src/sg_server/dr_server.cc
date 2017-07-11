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
#include "log/log.h"
#include "ceph_s3_meta.h"
#include "common/ceph_s3_api.h"
#include "common/db_meta_api.h"
#include "common/config_option.h"
#include "common/rpc_server.h"
#include "gc_task.h"
#include "writer_service.h"
#include "consumer_service.h"
#include "snapshot/snapshot_mgr.h"
#include "backup/backup_mgr.h"
#include "backup/backup_msg_handler.h"
#include "replicate/rep_inner_ctrl.h"
#include "volume_inner_control.h"
#include "replayer_context.h"
#include "replicate/rep_scheduler.h"
#include "replicate/task_handler.h"
#include "replicate/rep_task_generator.h"
#include "replicate/markers_maintainer.h"
#include "replicate/rep_message_handlers.h"
#include "transfer/net_sender.h"
#include "transfer/net_receiver.h"

#define DEFAULT_META_SERVER_PORT 50051
#define DEFAULT_REPLICATE_PORT 50061
#define MAX_TASK_COUNT_IN_QUEUE (128)

int main(int argc, char** argv) {
    std::string file="sg_server.log";
    DRLog::log_init(file);
    DRLog::set_log_level(SG_DEBUG);

    std::shared_ptr<KVApi> kvApi_ptr;
    if(g_option.global_storage_media.compare("ceph") == 0) {
        kvApi_ptr.reset(new CephS3Api(
                        g_option.ceph_s3_access_key.c_str(),
                        g_option.ceph_s3_secret_key.c_str(),
                        g_option.ceph_host.c_str(),
                        g_option.ceph_s3_bucket.c_str()));
    }
    else {
        string db_path = g_option.local_meta_path + "/journals/meta_db";
        if (access(db_path.c_str(), F_OK)) {
            char cmd[256] = "";
            snprintf(cmd, sizeof(cmd), "mkdir -p %s", db_path.c_str());
            int ret = system(cmd);
            SG_ASSERT(ret != -1);
        }
        IndexStore* index_store = IndexStore::create("rocksdb", db_path);
        kvApi_ptr.reset(new DBMetaApi(index_store));
    }

    std::shared_ptr<CephS3Meta> meta(new CephS3Meta(kvApi_ptr));
    std::shared_ptr<CephS3LeaseServer> lease_server(new CephS3LeaseServer);
    int gc_interval = g_option.lease_validity_window;
    lease_server->init(kvApi_ptr,gc_interval);

    string mount_path = g_option.journal_mount_point;

    // init meta server
    string ip1 = g_option.meta_server_ip;
    int port1 = g_option.meta_server_port;
    RpcServer metaServer(ip1,port1,grpc::InsecureServerCredentials());
    WriterServiceImpl writerSer(meta);
    ConsumerServiceImpl consumerSer(meta);
    VolInnerCtrl volInnerCtrl( meta /*VolumeMetaManager*/, meta /*JournalMetaManager*/);
    SnapshotMgr snapMgr;
    BackupMgr backupMgr;
    metaServer.register_service(&writerSer);
    metaServer.register_service(&consumerSer);
    metaServer.register_service(&volInnerCtrl);
    metaServer.register_service(&snapMgr);
    metaServer.register_service(&backupMgr);

    // init net receiver
    string ip2 = g_option.replicate_local_ip;
    int port2 = g_option.replicate_port;
    RpcServer repServer(ip2,port2,grpc::InsecureServerCredentials());

    RepMsgHandlers rep_msg_handler(meta/*JournalMetaManager*/,
                                   meta /*VolumeMetaManager*/,mount_path);
    BackupMsgHandler backup_msg_handler(backupMgr);
    NetReceiver netReceiver(rep_msg_handler, backup_msg_handler);
    repServer.register_service(&netReceiver);
    LOG_INFO << "net receiver server listening on " << ip2 << ":" << port2;
    if(!repServer.run()){
        LOG_FATAL << "start replicate server failed!";
        std::cerr << "start replicate server failed ip2:" << ip2 << "port2:" << port2 << std::endl;
        return -1;
    }

    // init replicate queues and work stages
    // repVolume queue, repScheduler sort & insert repVolume in it,and 
    // taskGenerator get repVolume from it
    BlockingQueue<std::shared_ptr<RepVolume>> rep_vol_que;
    // task queue: taskHandler handler the task
    std::shared_ptr<BlockingQueue<std::shared_ptr<TransferTask>>> task_que(
        new BlockingQueue<std::shared_ptr<TransferTask>>(MAX_TASK_COUNT_IN_QUEUE));
    // markerContext queue: markerMaintainer handle the marker
    std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>>
        marker_ctx_que(new BlockingQueue<std::shared_ptr<MarkerContext>>);

    // replicate second stage: generate repTasks
    TaskGenerator task_generator(rep_vol_que,task_que);

    // replicate third stage: transfer data to destination
    string addr = g_option.replicate_remote_ip;
    addr.append(":").append(std::to_string(port2));
    LOG_INFO << "transmitter connect to " << addr;
    NetSender::instance().init(grpc::CreateChannel(
        addr, grpc::InsecureChannelCredentials()));
    TaskHandler::instance().init(task_que,marker_ctx_que);

    // replicate forth stage: sync markers
    MarkersMaintainer::instance().init(marker_ctx_que);

    // init replicate scheduler: replicate first stage: sort repVolume
    RepScheduler rep_scheduler(meta,rep_vol_que);
    //init replicate control rpc server
    RepInnerCtrl rep_control(rep_scheduler,meta);
    metaServer.register_service(&rep_control);
    // start meta server
    LOG_INFO << "meta server listening on " << ip1 << ":" << port1;
    if(!metaServer.run()){
        LOG_FATAL << "start meta server failed!";
        std::cerr << "start meta server failed!" << std::endl;
        return -1;
    }

    // init gc thread
    GCTask::instance().init(lease_server,
        meta/*JournalGCManager*/,meta/*JournalMetaManager*/);
    // init volumes
    std::list<VolumeMeta> list;
    RESULT res = meta->list_volume_meta(list);
    if(DRS_OK == res){
        for(VolumeMeta& vol_meta:list){
            auto& vol = vol_meta.info().vol_id();
            GCTask::instance().add_volume(vol);
            ReplayerContext* c = (new ReplayerContext(vol,meta)); // delete memory when unregistered
            GCTask::instance().register_consumer(vol, c);
            if(vol_meta.info().role() == huawei::proto::REP_PRIMARY
                && vol_meta.info().rep_enable()) // no matter what replication status is
                rep_scheduler.add_volume(vol);

            /*snapshot meta init*/
            snapMgr.add_volume(vol_meta.info().vol_id(), vol_meta.info().size());
            /*backup meata init*/
            backupMgr.add_volume(vol_meta.info().vol_id(), vol_meta.info().size());
        }
        GCTask::instance().set_volumes_initialized(true);
    }
    else{
        LOG_ERROR << "get volume list failed!";
        std::cerr << "get volume list failed!" << std::endl;
    }
    metaServer.join();
    repServer.join();
    return 0;
}
