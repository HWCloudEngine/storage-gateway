#include "volume_manager.hpp"
#include <boost/bind.hpp>
#include <algorithm>
#include "connection.hpp"
#include "../log/log.h"

namespace Journal{

Volume::Volume(boost::asio::io_service& io_service)
    :raw_socket_(io_service),
     connection(raw_socket_, entry_queue, read_queue, reply_queue),
     pre_processor(entry_queue, write_queue),
     writer("localhost:50051", write_queue, reply_queue),
     reader(read_queue, reply_queue),
     replayer("localhost:50051")
{

}

Volume::~Volume()
{
    writer.deinit();
    reader.deinit();
    pre_processor.deinit();
    connection.deinit();
    
    if (buffer_pool != NULL)
    {
        nedalloc::neddestroypool(buffer_pool);
        buffer_pool = NULL;
    }
}

bool Volume::init(shared_ptr<ConfigParser> conf, shared_ptr<CephS3LeaseClient> lease_client)
{
    int thread_num = conf->get_default("pre_processor.thread_num",1);
    buffer_pool = nedalloc::nedcreatepool(BUFFER_POOL_SIZE,thread_num+2);
    if(buffer_pool == NULL)
    {
        LOG_ERROR << "create buffer pool failed";
        return false;
    }
    if(!connection.init(buffer_pool))
    {
        LOG_ERROR << "init connection failed,vol_id:" << vol_id_;
        return false;
    }
    if(!pre_processor.init(conf))
    {
        LOG_ERROR << "init pre_processor failed,vol_id:"<< vol_id_;
        return false;
    }

    idproxy.reset(new IDGenerator());
    cacheproxy.reset(new CacheProxy(vol_path_, idproxy));

    if(!writer.init(vol_id_, conf, idproxy, cacheproxy,lease_client))
    {
        LOG_ERROR << "init journal writer failed,vol_id:" << vol_id_;
        return false;
    }

    if(!reader.init(cacheproxy))
    {
        LOG_ERROR << "init journal writer failed,vol_id:" << vol_id_;
        return false;
    }
   
    if (!replayer.init(vol_id_, vol_path_, idproxy, cacheproxy)) 
	{
        LOG_ERROR << "init journal replayer failed,vol_id:" << vol_id_;
        return false;
    }

    return true;
}

void Volume::set_property(std::string vol_id,std::string vol_path)
{
    vol_id_ = vol_id;
    vol_path_ = vol_path;
}

raw_socket& Volume::get_raw_socket()
{
    return raw_socket_;
}

JournalWriter& Volume::get_writer()
{
    return writer;
}

void Volume::start()
{
    connection.start();
}

void Volume::stop()
{
    connection.stop();
}


//VolumeManager
VolumeManager::VolumeManager()
{
}

VolumeManager::~VolumeManager()
{
    thread_ptr->interrupt();
    thread_ptr->join();
}

bool VolumeManager::init()
{
    conf.reset(new ConfigParser(DEFAULT_CONFIG_FILE));
    lease_client.reset(new CephS3LeaseClient());
    std::string access_key,secret_key,host,bucket_name;
    access_key = conf->get_default("ceph_s3.access_key",access_key);
    secret_key = conf->get_default("ceph_s3.secret_key",secret_key);
    host = conf->get_default("ceph_s3.host",host);
    int renew_window = conf->get_default("ceph_s3.renew_window",4);
    int expire_window = conf->get_default("ceph_s3.expire_window",10);
    int validity_window = conf->get_default("ceph_s3.validity_window",2);
    bucket_name = conf->get_default("ceph_s3.bucket",bucket_name);
    
    lease_client->init(access_key.c_str(), secret_key.c_str(),
        host.c_str(), bucket_name.c_str(), renew_window,
        expire_window, validity_window) ;
    thread_ptr.reset(new boost::thread(boost::bind(&VolumeManager::periodic_task, this)));
}

void VolumeManager::periodic_task()
{
    int_least64_t interval = conf->get_default("ceph_s3.get_journal_interval",500);
    int journal_limit = conf->get_default("ceph_s3.journal_limit",4);

    while(true)
    {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(interval));
        if(!lease_client->check_lease_validity())
        {
            continue;
        }

        std::unique_lock<std::mutex> lk(mtx);
        for(std::map<std::string,volume_ptr>::iterator iter = volumes.begin();iter!=volumes.end();++iter)
        {
            std::string vol_id = iter->first;
            volume_ptr vol = iter->second;
            JournalWriter& writer = vol->get_writer();
            if(!writer.get_writeable_journals(lease_client->get_lease(),journal_limit))
            {
                LOG_ERROR << "get_writeable_journals failed,vol_id:" << vol_id;
            }

            if(!writer.seal_journals(lease_client->get_lease()))
            {
                LOG_ERROR << "seal_journals failed,vol_id:" << vol_id;
            }
            LOG_INFO << "seal_journals ok vol_id:" << vol_id;
        }
    }
}

void VolumeManager::add_vol(volume_ptr vol)
{
    LOG_INFO << "add vol";
    boost::asio::async_read(vol->get_raw_socket(),
    boost::asio::buffer(header_buffer_, sizeof(struct IOHookRequest)),
    boost::bind(&VolumeManager::handle_request_header, this,vol,
                 boost::asio::placeholders::error));
}

void VolumeManager::handle_request_header(volume_ptr vol,const boost::system::error_code& e)
{
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest *>(header_buffer_.data());
    if(!e && header_ptr->magic == MESSAGE_MAGIC )
    {
        if(header_ptr->type == ADD_VOLUME)
        {
                boost::asio::async_read(vol->get_raw_socket(),
                boost::asio::buffer(body_buffer_, sizeof(struct add_vol_req)),
                boost::bind(&VolumeManager::handle_request_body, this,vol,
                boost::asio::placeholders::error));
        }
        else
        {
            LOG_ERROR << "first message is not ADD_VOLUME";
        }
    }
    else
    {
        LOG_ERROR << "recieve header error:" << e << " ,magic number:" << header_ptr->magic;
    }

}

void VolumeManager::handle_request_body(volume_ptr vol,const boost::system::error_code& e)
{
    if(!e)
    {
        add_vol_req_t* body_ptr = reinterpret_cast<add_vol_req_t *>(body_buffer_.data());
        std::string vol_id = std::string(body_ptr->volume_name);
        std::string vol_path = std::string(body_ptr->device_path);
        std::unique_lock<std::mutex> lk(mtx);
        vol->set_property(vol_id,vol_path);
        volumes.insert(std::pair<std::string,volume_ptr>(vol_id,vol));
        bool ret = vol->init(conf,lease_client);
        send_reply(vol,ret);
        vol->start();
    }
    else
    {
        LOG_ERROR << "recieve add volume request data error:" << e;
    }
}

void VolumeManager::send_reply(volume_ptr vol,bool success)
{
    IOHookReply* reply_ptr = reinterpret_cast<IOHookReply *>(reply_buffer_.data());
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest *>(header_buffer_.data());
    reply_ptr->magic = MESSAGE_MAGIC;
    reply_ptr->error = success?0:1;
    reply_ptr->handle = header_ptr->handle;
    reply_ptr->len = 0;
    boost::asio::async_write(vol->get_raw_socket(),
    boost::asio::buffer(reply_buffer_, sizeof(struct IOHookReply)),
    boost::bind(&VolumeManager::handle_send_reply, this,
                 boost::asio::placeholders::error));
}

void VolumeManager::handle_send_reply(const boost::system::error_code& error)
{
    if (error)
    {
        std::cerr << "send reply failed";
    }
    else
    {
        ;
    }

}
    
void VolumeManager::start(volume_ptr vol)
{
    add_vol(vol);
}

void VolumeManager::stop(std::string vol_id)
{
    std::unique_lock<std::mutex> lk(mtx);
    std::map<std::string,volume_ptr>::iterator iter;
    iter = volumes.find(vol_id);
    if (iter != volumes.end())
    {
        volume_ptr vol = iter->second;
        vol->stop();
        volumes.erase(vol_id);
    }
}

void VolumeManager::stop_all()
{
    std::unique_lock<std::mutex> lk(mtx);
    for(std::map<std::string,volume_ptr>::iterator iter = volumes.begin();iter!=volumes.end();++iter)
    {
        volume_ptr vol = iter->second;
        vol->stop();
    }
    volumes.clear();
}
}
