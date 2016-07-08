#include "volume_manager.hpp"
#include <boost/bind.hpp>
#include <algorithm>
#include "connection.hpp"

namespace Journal{

Volume::Volume(boost::asio::io_service& io_service)
    :entry_queue_(),
     raw_socket_(io_service),
     handler(raw_socket_,entry_queue_),
     connection(raw_socket_,handler),
     writer()
{
    //todo read thread_num from config file
    int thread_num = 1;
	buffer_pool = nedalloc::nedcreatepool(BUFFER_POOL_SIZE,thread_num+2);
	if (buffer_pool == NULL)
	{
		//todo use LOG
		std::cout << "create buffer pool failed" << std::endl;
	}
    connection.init(buffer_pool);
    bool ret = handler.init(buffer_pool,thread_num);
    if(!ret)
    {
        std::cout << "init request handler failed" << std::endl;
    }
}

Volume::~Volume()
{
    handler.deinit();
    connection.deinit();
    
	if (buffer_pool != NULL)
	{
		nedalloc::neddestroypool(buffer_pool);
		buffer_pool = NULL;
	}
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
void VolumeManager::start(std::string vol_id,volume_ptr vol)
{
    volumes.insert(std::pair<std::string,volume_ptr>(vol_id,vol));
	vol->start();
}

void VolumeManager::stop(std::string vol_id)
{
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
    for(std::map<std::string,volume_ptr>::iterator iter = volumes.begin();iter!=volumes.end();++iter)
    {
        volume_ptr vol = iter->second;
        vol->stop();
    }
	volumes.clear();
}
}