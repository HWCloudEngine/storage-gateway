#include "volume_manager.h"
#include <boost/bind.hpp>
#include <algorithm>
#include "../log/log.h"
#include "control/control_snapshot.h"
#include "control/control_backup.h"
#include "control/control_replicate.h"
#include "control/control_volume.h"
#include "../common/volume_attr.h"

using huawei::proto::VolumeInfo;
using huawei::proto::StatusCode;

namespace Journal{

VolumeManager::~VolumeManager()
{
    running_ = false;

    thread_ptr->interrupt();
    thread_ptr->join();
    if(ctrl_rpc_server){
        ctrl_rpc_server->join(); 
        LOG_INFO << "stop ctrl rpc server ok";
        delete ctrl_rpc_server;
    }

    if(writer_thread->joinable()){
        writer_thread->join();
    }
    if(epoll_fd != -1){
        close(epoll_fd);
        epoll_fd = -1;
    }

    if(snapshot_ctrl){
        delete snapshot_ctrl; 
    }

    if(backup_ctrl){
        delete backup_ctrl; 
    }

    if(vol_ctrl){
        delete vol_ctrl;
    }

    if(rep_ctrl){
        delete rep_ctrl;
    }

    if(ep_events){
        delete ep_events;
    }
}

bool VolumeManager::init()
{
    conf.reset(new ConfigParser(DEFAULT_CONFIG_FILE));
    lease_client.reset(new CephS3LeaseClient());
    std::string access_key,secret_key,host,bucket_name;
    access_key = conf->get_default("ceph_s3.access_key",access_key);
    secret_key = conf->get_default("ceph_s3.secret_key",secret_key);
    host = conf->get_default("ceph_s3.host",host);
    int renew_window = conf->get_default("ceph_s3.lease_renew_window",100);
    int expire_window = conf->get_default("ceph_s3.lease_expire_window",600);
    int validity_window = conf->get_default("ceph_s3.lease_validity_window",150);
    bucket_name = conf->get_default("ceph_s3.bucket",bucket_name);
    producer_marker_update_interval = conf->get_default(
            "journal_writer.producer_marker_update_interval",5000);
    max_ep_events_num = conf->get_default("global.max_volume_count",128);

    ep_events = new epoll_event_t[max_ep_events_num];
    if(ep_events == nullptr){
        LOG_ERROR << "allocate epoll events failed!";
        DR_ERROR_OCCURED();
    }
    epoll_fd = epoll_create(512); // Since Linux 2.6.8, the size argument is ignored
    if(epoll_fd == -1){
        LOG_ERROR << "epoll create failed:" << errno;
        DR_ERROR_OCCURED();
    }

    lease_client->init(access_key.c_str(), secret_key.c_str(),
                       host.c_str(), bucket_name.c_str(), renew_window,
                       expire_window, validity_window) ;
    thread_ptr.reset(new boost::thread(boost::bind(&VolumeManager::periodic_task, this)));

    std::string default_ip("127.0.0.1");
    /*start rpc server for receive control command from sg control*/
    string ctrl_ip = conf->get_default("ctrl_server.ip", default_ip);
    int ctrl_port = conf->get_default("ctrl_server.port", 1111);
    ctrl_rpc_server = new RpcServer(ctrl_ip, ctrl_port, 
                                     grpc::InsecureServerCredentials());
    assert(ctrl_rpc_server!= nullptr);

    snapshot_ctrl = new SnapshotControlImpl(volumes); 
    assert(snapshot_ctrl != nullptr);
    ctrl_rpc_server->register_service(snapshot_ctrl);

    backup_ctrl = new BackupControlImpl(volumes); 
    assert(backup_ctrl != nullptr);
    ctrl_rpc_server->register_service(backup_ctrl);

    rep_ctrl = new ReplicateCtrl(volumes);
    ctrl_rpc_server->register_service(rep_ctrl);

    std::string svr_ip = conf->get_default("meta_server.ip", default_ip);
    int svr_port = conf->get_default("meta_server.port", 50051);
    svr_ip += ":" + std::to_string(svr_port);
    writer_rpc_client.reset(new WriterClient(
            grpc::CreateChannel(svr_ip,grpc::InsecureChannelCredentials())));
    vol_inner_client_.reset(
            new VolInnerCtrlClient(
                    grpc::CreateChannel(svr_ip,
                            grpc::InsecureChannelCredentials())));
    vol_ctrl = new VolumeControlImpl(host_, port_,vol_inner_client_);
    ctrl_rpc_server->register_service(vol_ctrl);

    if(!ctrl_rpc_server->run()){
        LOG_FATAL << "start ctrl rpc server failed!";
        return false;
    }
    LOG_INFO << "start ctrl rpc server ok";

    writer_thread.reset(new std::thread(&VolumeManager::writer_thread_work,this));
    return true;
}

void VolumeManager::periodic_task()
{
    int_least64_t interval = conf->get_default("ceph_s3.get_journal_interval",500);
    int journal_limit = conf->get_default("ceph_s3.journal_limit",4);

    while(running_){
        boost::this_thread::sleep_for(boost::chrono::milliseconds(interval));
        std::string lease_uuid = lease_client->get_lease();
        if(!lease_client->check_lease_validity(lease_uuid)){
            continue;
        }

        std::unique_lock<std::mutex> lk(mtx);
        for(auto iter : volumes){
            std::string vol_id = iter.first;
            auto vol = iter.second;
            std::shared_ptr<JournalWriter> writer = vol->get_writer();
            if(!writer->get_writeable_journals(lease_uuid,journal_limit)){
                LOG_ERROR << "get_writeable_journals failed,vol_id:" << vol_id;
            }

            if(!writer->seal_journals(lease_uuid)){
                LOG_ERROR << "seal_journals failed,vol_id:" << vol_id;
            }
        }
    }
}

void VolumeManager::read_req_head_cbt(raw_socket_t client_sock,
                                      const char* req_head_buffer,
                                      const boost::system::error_code& e)
{
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest*>
                                (const_cast<char*>(req_head_buffer));
    if(!e && header_ptr->magic == MESSAGE_MAGIC){
        if(header_ptr->type == ADD_VOLUME){
            char* req_body_buffer = new char[sizeof(struct add_vol_req)];
            boost::asio::async_read(*client_sock,
                boost::asio::buffer(req_body_buffer, sizeof(struct add_vol_req)),
                boost::bind(&VolumeManager::read_req_body_cbt, this, 
                             client_sock, 
                             req_head_buffer,
                             req_body_buffer,
                             boost::asio::placeholders::error));
        } else {
            LOG_ERROR << "first message is not ADD_VOLUME";
        }
    } else {
        LOG_ERROR << "recieve header error:" << e << " magic number:" << header_ptr->magic;
    }
}

void VolumeManager::read_req_body_cbt(raw_socket_t client_sock,
                                      const char* req_head_buffer,
                                      const char* req_body_buffer,
                                      const boost::system::error_code& e)
{
    if(e){
        LOG_ERROR << "recieve add volume request data error:" << e;
        return;
    }

    add_vol_req_t* body_ptr = reinterpret_cast<add_vol_req_t*>
                              (const_cast<char*>(req_body_buffer));
    std::string vol_name = std::string(body_ptr->volume_name);
    std::string dev_path = std::string(body_ptr->device_path);
    
    LOG_INFO << "add volume:" << vol_name << " dev_path:" << dev_path;

    VolumeInfo volume_info;
    StatusCode ret = vol_inner_client_->get_volume(vol_name, volume_info);
    if (ret == StatusCode::sOk)
    {
        LOG_INFO << "get volume:" << volume_info.vol_id() 
                 << " dev_path:" << volume_info.path()
                 << " vol_status:" << volume_info.vol_status()
                 << " vol_size:" << volume_info.size();

        VolumeAttr volume_attr(volume_info);

        /*create volume*/
        shared_ptr<Volume> vol = make_shared<Volume>(client_sock, volume_attr,
                conf, lease_client,writer_rpc_client,epoll_fd);
        /*add to map*/
        std::unique_lock<std::mutex> lk(mtx);
        volumes.insert({vol_name, vol});

        /*reply to tgt client*/
        send_reply(client_sock, req_head_buffer, req_body_buffer, true);

        /*volume init*/
        vol->init();        
        /*volume start, start receive io from network*/
        vol->start();
    }
    else
    {
        LOG_INFO << "get volume failed";
        /*reply to tgt client*/
        send_reply(client_sock, req_head_buffer, req_body_buffer, false);
    }
}

void VolumeManager::send_reply(raw_socket_t client_sock, 
                               const char* req_head_buffer, 
                               const char* req_body_buffer, bool success)
{
    char* rep_buffer = new char[sizeof(IOHookReply)]; 
    IOHookReply* reply_ptr = reinterpret_cast<IOHookReply*>(rep_buffer);
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest*>
                                (const_cast<char*>(req_head_buffer));
    reply_ptr->magic = MESSAGE_MAGIC;
    reply_ptr->error = success ? 0 : 1;
    reply_ptr->handle = header_ptr->handle;
    reply_ptr->len = 0;
    boost::asio::async_write(*client_sock,
        boost::asio::buffer(rep_buffer, sizeof(struct IOHookReply)),
        boost::bind(&VolumeManager::send_reply_cbt, this,
                    req_head_buffer, req_body_buffer, rep_buffer,
                    boost::asio::placeholders::error));
}

void VolumeManager::send_reply_cbt(const char* req_head_buffer,
                                   const char* req_body_buffer,
                                   const char* rep_buffer,
                                   const boost::system::error_code& error)
{
    if (error){
        std::cerr << "send reply failed";
    }
    
    delete [] rep_buffer;
    delete [] req_body_buffer;
    delete [] req_head_buffer;
}

void VolumeManager::start(raw_socket_t client_sock)
{
    /*prepare to read add volume request*/
    /*will be free after send reply to client*/
    char* req_head_buffer = new char[sizeof(IOHookRequest)];
    boost::asio::async_read(*client_sock,
        boost::asio::buffer(req_head_buffer, sizeof(struct IOHookRequest)),
        boost::bind(&VolumeManager::read_req_head_cbt, this, 
                     client_sock, req_head_buffer,
                     boost::asio::placeholders::error));
}

void VolumeManager::stop(std::string vol_id)
{
    std::unique_lock<std::mutex> lk(mtx);
    auto iter = volumes.find(vol_id);
    if (iter != volumes.end()){
        auto vol = iter->second;
        vol->fini();
        vol->stop();
        volumes.erase(vol_id);
    }
}

void VolumeManager::stop_all()
{
    std::unique_lock<std::mutex> lk(mtx);
    for(auto it : volumes){
        auto vol = it.second;
        vol->fini();
        vol->stop();
    }
    volumes.clear();
}

// update producer marker related methods
bool is_markers_equal(const JournalMarker& cur, const JournalMarker& pre){
    if(cur.pos() != pre.pos())
        return false;
    return (cur.cur_journal().compare(pre.cur_journal()) == 0);
}

void VolumeManager::update_producer_markers(
        std::map<string,JournalMarker>& markers_to_update){
    if(markers_to_update.empty())
        return;
    bool res = writer_rpc_client->update_multi_producer_markers(
            lease_client->get_lease(),markers_to_update);
    if(res){
        // update last producer markers if update successfully
        for(auto it=markers_to_update.begin(); it!=markers_to_update.end();it++){
            last_producer_markers[it->first] = it->second;
            LOG_DEBUG << "update producer marker: "
                << it->second.cur_journal() << ":" << it->second.pos();
        }
    }
    else{ // if failed, not update last maker
        LOG_ERROR << "update producer markers failed!";
    }
}

void VolumeManager::writer_thread_work(){
    while(running_){
        int ret = epoll_wait(epoll_fd,ep_events,
                max_ep_events_num,producer_marker_update_interval);
        if(ret > 0){
            std::map<string,JournalMarker> markers_to_update;
            for(int i=0;i<ret;i++){
                JournalWriter* writer = (JournalWriter*)ep_events[i].data.ptr;
                SG_ASSERT(writer!=nullptr);
                writer->clear_producer_event();
                
                if(writer->is_producer_marker_holding()){
                    continue;
                }
                JournalMarker marker = writer->get_cur_producer_marker();
                auto it = last_producer_markers.find(writer->get_vol_id());
                if(it != last_producer_markers.end()){
                    // if marker not changed, no need to update
                    if(is_markers_equal(marker,it->second)){
                        continue;
                    }
                }
                // last one not found or changed, need to update
                markers_to_update.insert(std::pair<string,JournalMarker>(it->first,marker));
            }
            LOG_DEBUG << "invoked to update producer marker";
            update_producer_markers(markers_to_update);
        }
        else if(ret == 0){// timeout, try to update producer marker of all volumes
            update_all_producer_markers();
        }
        else{ // error occured
            LOG_ERROR << "epoll wait error in writer_thread, error code:" << errno;
        }
    }
}

int  VolumeManager::update_all_producer_markers(){
    std::map<string,JournalMarker> markers_to_update;
    for(auto it=volumes.begin();it!=volumes.end();it++){
        std::shared_ptr<JournalWriter> writer = it->second->get_writer();
        if(nullptr == writer){
            LOG_WARN << "writer of volume [" << it->first << "] not init.";
            continue;
        }
        if(!writer->is_producer_marker_holding()){
            JournalMarker marker = writer->get_cur_producer_marker();
            auto it2 = last_producer_markers.find(it->first);
            if(it2 != last_producer_markers.end()){
                if(is_markers_equal(marker,it2->second))
                    continue;
            }
            markers_to_update.insert(std::pair<string,JournalMarker>(it->first,marker));
        }
    }
    update_producer_markers(markers_to_update);
}
}
