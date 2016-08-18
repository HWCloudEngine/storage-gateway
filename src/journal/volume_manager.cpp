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
     writer("localhost:50051",write_queue_,raw_socket_,write_cv)
{
}

Volume::~Volume()
{
    writer.deinit();
    pre_processor.deinit();
    connection.deinit();
    
    if (buffer_pool != NULL)
    {
        nedalloc::neddestroypool(buffer_pool);
        buffer_pool = NULL;
    }
}
bool Volume::init(std::string& vol)
{
    //todo read thread_num from config file
    vol_id = vol;
    int thread_num = 1;
    buffer_pool = nedalloc::nedcreatepool(BUFFER_POOL_SIZE,thread_num+2);
    if(buffer_pool == NULL)
    {
        LOG_ERROR << "create buffer pool failed";
        return false;
    }
    if(!connection.init(buffer_pool))
    {
        LOG_ERROR << "init connection failed,vol_id:" << vol;
        return false;
    }
    if(!pre_processor.init(buffer_pool,thread_num))
    {
        LOG_ERROR << "init pre_processor failed,vol_id:"<< vol;
        return false;
    }
    if(!writer.init(vol))
    {
        LOG_ERROR << "init journal writer failed,vol_id:" << vol;
        return false;
    }
    return true;
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
    
void VolumeManager::start(std::string vol_id,volume_ptr vol)
{
    std::unique_lock<std::mutex> lk(mtx);
    volumes.insert(std::pair<std::string,volume_ptr>(vol_id,vol));
    vol->start();
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