/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    volume_manage.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  volume management
*
*************************************************/
#include <fstream>
#include <algorithm>
#include <boost/bind.hpp>
#include "log/log.h"
#include "common/volume_attr.h"
#include "common/utils.h"
#include "control/control_snapshot.h"
#include "control/control_backup.h"
#include "control/control_replicate.h"
#include "control/control_volume.h"
#include "volume_manager.h"
#include "rpc/clients/rpc_client.h"


using huawei::proto::VolumeInfo;
using huawei::proto::StatusCode;
VolumeManager::VolumeManager(const std::string& host, const std::string& port):
        host_(host), port_(port),running_(true)
{
    interval = 0;
    journal_limit = 0;
    epoll_fd = -1;
    ep_events = 0;
    max_ep_events_num = 0;
    producer_marker_update_interval = 0;
}
 
VolumeManager::~VolumeManager()
{
    running_ = false;
    if(recover_targets_thr_){
        recover_targets_thr_->join();
    }
    thread_ptr->interrupt();
    thread_ptr->join();
    if(ctrl_rpc_server){
        ctrl_rpc_server->join(); 
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
    lease_client.reset(new CephS3LeaseClient());
    producer_marker_update_interval = g_option.journal_producer_marker_update_interval;
    max_ep_events_num = 100;
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

    lease_client->init(g_option.lease_renew_window,
                       g_option.lease_expire_window,
                       g_option.lease_validity_window) ;

    thread_ptr.reset(new boost::thread(boost::bind(&VolumeManager::periodic_task, this)));

    std::string default_ip("127.0.0.1");
    /*start rpc server for receive control command from sg control*/
    ctrl_rpc_server = new RpcServer(g_option.ctrl_server_ip, 
                                    g_option.ctrl_server_port, 
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

    std::string meta_rpc_addr = rpc_address(g_option.meta_server_ip, g_option.meta_server_port);
    vol_inner_client_.reset(new VolInnerCtrlClient(grpc::CreateChannel(meta_rpc_addr,
                    grpc::InsecureChannelCredentials())));
    vol_ctrl = new VolumeControlImpl(host_, port_,vol_inner_client_, *this);
    ctrl_rpc_server->register_service(vol_ctrl);

    init_volumes();

    if(!ctrl_rpc_server->run()){
        LOG_FATAL << "start ctrl rpc server failed!";
        return false;
    }
    LOG_INFO << "start ctrl rpc server ok";

    writer_thread.reset(new std::thread(&VolumeManager::writer_thread_work,this));
    return true;
}

void VolumeManager::init_volumes()
{
    LOG_INFO << "init volumes";
    std::ifstream f(g_option.local_volumes_conf);
    if(!f.is_open())
    {
        return;
    }
    std::string vol_name;
    while(getline(f,vol_name))
    {
        if(vol_name.empty())
        {
            LOG_INFO <<"persistent volume info is invalid,info:"<<vol_name;
            continue;
        }
        VolumeInfo volume_info;
        StatusCode ret = vol_inner_client_->get_volume(vol_name, volume_info);
        if (ret != StatusCode::sOk)
        {
            LOG_INFO <<"get volume info from sg server failed"<<vol_name;
            continue;
        }
        add_volume(volume_info, true);
    }
    f.close();
    LOG_INFO << "init volumes ok";
}

void VolumeManager::periodic_task()
{
    while(running_){
        boost::this_thread::sleep_for(boost::chrono::milliseconds(g_option.journal_interval));
        std::string lease_uuid = lease_client->get_lease();
        if(!lease_client->check_lease_validity(lease_uuid)){
            continue;
        }

        std::unique_lock<std::mutex> lk(mtx);
        for(auto iter : volumes){
            std::string vol_id = iter.first;
            auto vol = iter.second;

            std::shared_ptr<JournalWriter> writer = vol->get_writer();
            VolumeAttr& vol_attr = writer->get_vol_attr();
            if(vol_attr.is_writable()){
                if(!writer->get_writeable_journals(lease_uuid,g_option.journal_limit)){
                    LOG_ERROR << "get_writeable_journals failed,vol_id:" << vol_id;
                }

                if(!writer->seal_journals(lease_uuid)){
                    LOG_ERROR << "seal_journals failed,vol_id:" << vol_id;
                }
            }
        }
    }
}

void VolumeManager::read_req_head_cbt(raw_socket_t client_sock,
                                      const char* req_head_buffer,
                                      const boost::system::error_code& e)
{
    io_request_t* header_ptr = reinterpret_cast<io_request_t*>
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

bool VolumeManager::deinit_socket(const std::string &vol_name)
{
    LOG_INFO << "get volume obj:" << vol_name;
    auto iter = volumes.find(vol_name);
    if (iter == volumes.end())
    {
        LOG_INFO << "get volume obj:" << vol_name << " is not exist";
        return true;
    }
    iter->second->deinit_socket();
    return true;
}

shared_ptr<Volume> VolumeManager::add_volume(const VolumeInfo& volume_info, bool recover)
{
    shared_ptr<Volume> vol;
    std::unique_lock<std::mutex> lk(mtx);
    std::string vol_name = volume_info.vol_id();
    LOG_INFO << "add volume:" << vol_name;
    auto iter = volumes.find(vol_name);
    if (iter == volumes.end())
    {
        /*create volume*/
        LOG_INFO << "create volume obj:" << vol_name;
        vol = make_shared<Volume>(*this, volume_info, lease_client, epoll_fd);
        vol->init();
        volumes.insert({vol_name, vol});
        if (!recover)
        {
            persist_volume(vol_name);
        }
    }
    else
    {
        vol = iter->second;
        vol->update_volume_attr(volume_info);
    }
    LOG_INFO << "add volume:" << vol_name <<" ok";
    return vol;
}

bool VolumeManager::persist_volume(const std::string& vol_name)
{
    LOG_INFO << "persist volume:" << vol_name << " to conf file";
    std::ifstream fin(g_option.local_volumes_conf);
    if(!fin.is_open())
    {
        LOG_INFO << " open volumes conf file failed";
        return false;
    }
    std::string s((std::istreambuf_iterator<char>(fin)), std::istreambuf_iterator<char>());
    fin.close();
    std::string::size_type pos = s.find(vol_name);
    if(pos != std::string::npos)
    {
        LOG_INFO << vol_name <<" already persist in volumes.conf file";
        return true;
    }
    std::ofstream fout(g_option.local_volumes_conf, std::ios::app);
    if(!fout.is_open())
    {
        LOG_INFO <<" open volumes conf file failed";
        return false;
    }
    fout<< vol_name << std::endl;
    fout.close();
    LOG_INFO << "persist volume:" << vol_name << " to conf file ok";
    return true;
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
    if(ret != StatusCode::sOk){
        send_reply(client_sock, req_head_buffer, req_body_buffer, false);
        return;
    }
 
    LOG_INFO << "get volume:" << volume_info.vol_id() << " dev_path:" << volume_info.path()
             << " vol_status:" << volume_info.vol_status() << " vol_size:" << volume_info.size();
    shared_ptr<Volume> vol = add_volume(volume_info);

    /*reply to tgt client*/
    send_reply(client_sock, req_head_buffer, req_body_buffer, true);

    /*volume init*/
    vol->init_socket(client_sock);
    /*volume start, start receive io from network*/
    vol->start();
}

void VolumeManager::send_reply(raw_socket_t client_sock, 
                               const char* req_head_buffer, 
                               const char* req_body_buffer, bool success)
{
    char* rep_buffer = new char[sizeof(io_reply_t)]; 
    io_reply_t* reply_ptr = reinterpret_cast<io_reply_t*>(rep_buffer);
    io_request_t* header_ptr = reinterpret_cast<io_request_t*>
                                (const_cast<char*>(req_head_buffer));
    reply_ptr->magic = MESSAGE_MAGIC;
    reply_ptr->error = success ? 0 : 1;
    reply_ptr->handle = header_ptr->handle;
    reply_ptr->len = 0;
    boost::asio::async_write(*client_sock,
        boost::asio::buffer(rep_buffer, sizeof(io_reply_t)),
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
    char* req_head_buffer = new char[sizeof(io_request_t)];
    boost::asio::async_read(*client_sock,
        boost::asio::buffer(req_head_buffer, sizeof(io_request_t)),
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
    StatusCode res = g_rpc_client.update_multi_producer_markers(
            lease_client->get_lease(),markers_to_update);
    if(res == StatusCode::sOk){
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
                MarkerHandler* marker_handler = (MarkerHandler*)ep_events[i].data.ptr;
                SG_ASSERT(marker_handler!=nullptr);
                marker_handler->clear_producer_event();
                
                if(marker_handler->is_producer_marker_holding()){
                    continue;
                }
                JournalMarker marker = marker_handler->get_cur_producer_marker();
                auto it = last_producer_markers.find(marker_handler->get_vol_attr().vol_name());
                if(it != last_producer_markers.end()){
                    // if marker not changed, no need to update
                    if(is_markers_equal(marker,it->second)){
                        continue;
                    }
                }
                // last one not found or changed, need to update
                markers_to_update.insert({marker_handler->get_vol_attr().vol_name(), marker});

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
    std::unique_lock<std::mutex> lk(mtx);
    for(auto it=volumes.begin();it!=volumes.end();it++){
        std::shared_ptr<JournalWriter> writer = it->second->get_writer();
        if(nullptr == writer){
            LOG_WARN << "writer of volume [" << it->first << "] not init.";
            continue;
        }
        MarkerHandler& marker_handler = writer->get_maker_handler();
        if(!marker_handler.is_producer_marker_holding()){
            JournalMarker marker = marker_handler.get_cur_producer_marker();
            auto it2 = last_producer_markers.find(it->first);
            if(it2 != last_producer_markers.end()){
                if(is_markers_equal(marker,it2->second))
                    continue;
            }
            markers_to_update.insert(std::pair<string,JournalMarker>(it->first,marker));
        }
    }
    update_producer_markers(markers_to_update);
    return 0;
}

bool VolumeManager::del_volume(const string& vol)
{
    LOG_INFO << "delete volume:" << vol;
    std::unique_lock<std::mutex> lk(mtx);
    auto it = volumes.find(vol);
    if(it == volumes.end()){
        LOG_ERROR << "del volume vol:" << vol << "not exist";
        return false;
    }
    
    LOG_INFO << "stop volume:" << vol;
    it->second->stop();
    LOG_INFO << "stop volume:" << vol << " ok";

    volumes.erase(vol);
    remove_volume(vol);
    LOG_INFO << "delete volume:" << vol << " ok";
    return true;
}

bool VolumeManager::remove_volume(const std::string &vol_name)
{
    LOG_INFO << "remove volume:" << vol_name << " from conf file";
    std::ifstream fin(g_option.local_volumes_conf);
    if(!fin.is_open())
    {
        LOG_INFO <<" open volumes conf file failed";
        return false;
    }
    std::string s((std::istreambuf_iterator<char>(fin)), std::istreambuf_iterator<char>());
    fin.close();
    std::string::size_type pos = s.find(vol_name);
    if(pos == std::string::npos)
    {
        LOG_INFO << vol_name <<" not found in volumes conf file";
        return true;
    }
    s.erase(pos, vol_name.size()+1);
    std::ofstream fout(g_option.local_volumes_conf);
    if(!fout.is_open())
    {
        LOG_INFO <<" open volumes conf file failed";
        return false;
    }
    fout<<s;
    fout.close();
    LOG_INFO << "remove volume:" << vol_name << " from conf file ok";
    return true;
}

bool VolumeManager::recover_targets()
{
    if(vol_ctrl == nullptr){
        LOG_ERROR << "volume ctrl serivice not ok"; 
        return false;
    }
    recover_targets_thr_ = make_shared<thread>(std::bind(&VolumeControlImpl::recover_targets, vol_ctrl));
    return true;
}
