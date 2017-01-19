#include "volume_manager.h"
#include <boost/bind.hpp>
#include <algorithm>
#include "../log/log.h"
#include "snapshot_control.h"
#include "replicate_control.h"

namespace Journal{

VolumeManager::~VolumeManager()
{
    thread_ptr->interrupt();
    thread_ptr->join();
    if(ctrl_rpc_server){
        ctrl_rpc_server->join(); 
        LOG_INFO << "stop ctrl rpc server ok";
        delete ctrl_rpc_server;
    }

    if(ctrl_service){
        delete ctrl_service; 
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
    
    lease_client->init(access_key.c_str(), secret_key.c_str(),
                       host.c_str(), bucket_name.c_str(), renew_window,
                       expire_window, validity_window) ;
    thread_ptr.reset(new boost::thread(boost::bind(&VolumeManager::periodic_task, this)));
    
    /*start rpc server for receive control command from sg control*/
    ctrl_rpc_server = new RpcServer("127.0.0.1", 1111, 
                                     grpc::InsecureServerCredentials());
    assert(ctrl_rpc_server!= nullptr);

    ctrl_service = new SnapshotControlImpl(volumes); 
    assert(ctrl_service!= nullptr);

    ctrl_rpc_server->register_service(ctrl_service);

    ReplicateCtrl rep_ctrl(ctrl_service);
    ctrl_rpc_server->register_service(&rep_ctrl);
    
    if(!ctrl_rpc_server->run()){
        LOG_FATAL << "start ctrl rpc server failed!";
        return false;
    }
    LOG_INFO << "start ctrl rpc server ok";
    return true;
}

void VolumeManager::periodic_task()
{
    int_least64_t interval = conf->get_default("ceph_s3.get_journal_interval",500);
    int journal_limit = conf->get_default("ceph_s3.journal_limit",4);

    while(true){
        boost::this_thread::sleep_for(boost::chrono::milliseconds(interval));
        std::string lease_uuid = lease_client->get_lease();
        if(!lease_client->check_lease_validity(lease_uuid)){
            continue;
        }

        std::unique_lock<std::mutex> lk(mtx);
        for(auto iter : volumes){
            std::string vol_id = iter.first;
            volume_ptr vol = iter.second;
            JournalWriter& writer = vol->get_writer();
            if(!writer.get_writeable_journals(lease_uuid,journal_limit)){
                LOG_ERROR << "get_writeable_journals failed,vol_id:" << vol_id;
            }

            if(!writer.seal_journals(lease_uuid)){
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
    /*create volume*/
    shared_ptr<Volume> vol = make_shared<Volume>(client_sock, vol_name, 
                                                 dev_path, conf, lease_client);
    /*add to map*/
    std::unique_lock<std::mutex> lk(mtx);
    volumes.insert(std::pair<std::string,volume_ptr>(vol_name,vol));

    /*reply to tgt client*/
    send_reply(client_sock, req_head_buffer, req_body_buffer, true);
    
    /*volume init*/
    vol->init();
    /*volume start, start receive io from network*/
    vol->start();
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
        volume_ptr vol = iter->second;
        vol->fini();
        vol->stop();
        volumes.erase(vol_id);
    }
}

void VolumeManager::stop_all()
{
    std::unique_lock<std::mutex> lk(mtx);
    for(auto it : volumes){
        volume_ptr vol = it.second;
        vol->fini();
        vol->stop();
    }
    volumes.clear();
}

}
