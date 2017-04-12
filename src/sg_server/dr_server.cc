#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sys/types.h>
#include <ifaddrs.h>
#include <sys/socket.h> //getnameinfo
#include <netdb.h> //NI_MAXHOST
#include <unistd.h>
#include "log/log.h"
#include "ceph_s3_meta.h"
#include "common/config_parser.h"
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
#include "tooz_client.h"

#define DEFAULT_META_SERVER_PORT 50051
#define DEFAULT_REPLICATE_PORT 50061
#define MAX_TASK_COUNT_IN_QUEUE (128)

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
    std::string file="sg_server.log";
    DRLog::log_init(file);
    DRLog::set_log_level(SG_DEBUG);
    Configure conf;
    conf.init(DEFAULT_CONFIG_FILE);
    std::shared_ptr<CephS3Meta> meta(new CephS3Meta(conf));

    string type = conf.global_journal_data_storage;
    string mount_path = conf.journal_mount_point;
    if(type.compare("ceph_fs") != 0 || mount_path.empty()){        
        LOG_FATAL << "config parse ceph_fs.mount_point error!";
        std::cerr << "config parse ceph_fs.mount_point error!" << std::endl;
        return -1;
    }

    // init meta server
    string ip1 = conf.meta_server_ip;
    int port1 = conf.meta_server_port;
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

    //multi-instance
    ToozClient tc;
    string backend_url = conf.cluster_backend_url;
    string group_id = conf.cluster_group_id;
    stringstream member_id_ss;
    member_id_ss << conf.meta_server_ip << ":" << conf.meta_server_port;
    string member_id = member_id_ss.str();
    LOG_INFO << "backend_url=" << backend_url << " group_id=" << group_id << " member_id=" << member_id;
    tc.start_coordination(backend_url, member_id);
    tc.join_group(group_id);
    while(tc.get_cluster_size(group_id) < conf.cluster_server_number){
        LOG_DEBUG << "wait other server join group";
        sleep(5);
    }
    tc.watch_group(group_id);
    tc.rehash_buckets_to_node(group_id);
    LOG_INFO << "multi_instance start";
    tc.register_callback(&backupMgr);
    LOG_INFO << "manager buckets size=" << tc.get_buckets().size();


    // init net receiver
    string ip2 = conf.replicate_local_ip;
    int port2 = conf.replicate_port;
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
    string addr = conf.replicate_remote_ip;
    addr.append(":").append(std::to_string(port2));
    LOG_INFO << "transmitter connect to " << addr;
    NetSender::instance().init(grpc::CreateChannel(
        addr, grpc::InsecureChannelCredentials()));
    TaskHandler::instance().init(task_que,marker_ctx_que);

    // replicate forth stage: sync markers
    MarkersMaintainer::instance().init(marker_ctx_que);

    // init replicate scheduler: replicate first stage: sort repVolume
    RepScheduler rep_scheduler(meta,mount_path,rep_vol_que);
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
    GCTask::instance().init(conf, meta/*JournalGCManager*/,meta/*JournalMetaManager*/);
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
    tc.stop_coordination();
    return 0;
}
