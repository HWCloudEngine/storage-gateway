#include "volume_manager.hpp"
#include <boost/bind.hpp>
#include <algorithm>
#include "connection.hpp"
#include "../log/log.h"

namespace Journal{

Volume::Volume(boost::asio::io_service& io_service)
    :write_queue_(),
     entry_queue_(),
     raw_socket_(io_service),
     pre_processor(raw_socket_,write_queue_,entry_queue_,entry_cv,write_cv),
     connection(raw_socket_,entry_queue_,entry_cv),
     writer("localhost:50051",write_queue_,raw_socket_,write_cv),
     replayer("localhost:50051")
{
}

Volume::~Volume()
{
    writer.deinit();
    replayer.deinit();
    pre_processor.deinit();
    connection.deinit();
    
    if (buffer_pool != NULL)
    {
        nedalloc::neddestroypool(buffer_pool);
        buffer_pool = NULL;
    }
}
bool Volume::init()
{
    //todo read thread_num from config file
    int thread_num = 1;
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
    if(!pre_processor.init(buffer_pool,thread_num))
    {
        LOG_ERROR << "init pre_processor failed,vol_id:"<< vol_id_;
        return false;
    }
    if(!writer.init(vol_id_))
    {
        LOG_ERROR << "init journal writer failed,vol_id:" << vol_id_;
        return false;
    }
    id_maker_ptr_.reset(new IDGenerator());
    cache_proxy_ptr_.reset(new CacheProxy(vol_path_, id_maker_ptr_));
    if (!replayer.init(vol_id_, vol_path_)) {
        LOG_ERROR<< "init journal replayer failed,vol_id:" << vol_id_;
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
    thread_ptr.reset(new boost::thread(boost::bind(&VolumeManager::periodic_task, this)));
}

void VolumeManager::periodic_task()
{
    //todo read config.ini
    int_least64_t interval = 500;
    int journal_limit = 4;
    while(true)
    {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(interval));
        std::unique_lock<std::mutex> lk(mtx);
        for(std::map<std::string,volume_ptr>::iterator iter = volumes.begin();iter!=volumes.end();++iter)
        {
            std::string vol_id = iter->first;
            volume_ptr vol = iter->second;
            JournalWriter& writer = vol->get_writer();
            if(!writer.get_writeable_journals("test-uuid",journal_limit))
            {
                LOG_ERROR << "get_writeable_journals failed,vol_id:" << vol_id;
            }
            if(!writer.seal_journals("test-uuid"))
            {
                LOG_ERROR << "seal_journals failed,vol_id:" << vol_id;
            }
        }
    }
}

void VolumeManager::add_vol(volume_ptr vol)
{
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
            //todo
            std::cerr << "first message is not ADD_VOLUME";
        }
    }
    else
    {
        //todo bad request
        ;
    }

}

void VolumeManager::handle_request_body(volume_ptr vol,const boost::system::error_code& e)
{
    if(!e)
    {
        add_vol_req_t* header_ptr = reinterpret_cast<add_vol_req_t *>(body_buffer_.data());
        std::string vol_id = std::string(header_ptr->volume_name);
        std::string vol_path = std::string(header_ptr->device_path);
        std::unique_lock<std::mutex> lk(mtx);
        vol->set_property(vol_id,vol_path);
        volumes.insert(std::pair<std::string,volume_ptr>(vol_id,vol));
        vol->init();
        vol->start();
    }
    else
    {
        //todo
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
