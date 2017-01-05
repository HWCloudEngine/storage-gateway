#include <boost/bind.hpp>
#include <algorithm>
#include "../log/log.h"
#include "volume.hpp"

namespace Journal{

Volume::Volume(raw_socket_t client_sock, 
               const string& vol_name, const string& dev_path,
               shared_ptr<ConfigParser> conf, 
               shared_ptr<CephS3LeaseClient> lease_client)
                :raw_socket_(client_sock), 
                 vol_id_(vol_name), vol_path_(dev_path), 
                 conf_(conf), lease_client_(lease_client)
{
}

Volume::~Volume()
{
    fini();
}

bool Volume::init()                  
{
    int thread_num = conf_->get_default("pre_processor.thread_num",1);
    buffer_pool_ = nedalloc::nedcreatepool(BUFFER_POOL_SIZE,thread_num+2);
    if(buffer_pool_ == NULL){
        LOG_ERROR << "create buffer pool failed";
        return false;
    }

    idproxy_.reset(new IDGenerator());
    cacheproxy_.reset(new CacheProxy(vol_path_, idproxy_));
    snapshotproxy_.reset(new SnapshotProxy(vol_id_, vol_path_, entry_queue_)); 

    connection_.reset(new Connection(raw_socket_, 
                                     entry_queue_, read_queue_, reply_queue_));
    pre_processor_.reset(new JournalPreProcessor(entry_queue_, write_queue_));
    reader_.reset(new JournalReader(read_queue_, reply_queue_));
    writer_.reset(new JournalWriter(write_queue_, reply_queue_));
    replayer_.reset(new JournalReplayer(vol_status_));

    if(!connection_->init(buffer_pool_)){
        LOG_ERROR << "init connection failed,vol_id:" << vol_id_;
        return false;
    }

    if(!pre_processor_->init(conf_)){
        LOG_ERROR << "init pre_processor failed,vol_id:"<< vol_id_;
        return false;
    }
    
    /*todo read from config*/
    if(!writer_->init(vol_id_, string("localhost:50051"), conf_, 
                     idproxy_, cacheproxy_, snapshotproxy_, 
                     lease_client_)){
        LOG_ERROR << "init journal writer failed,vol_id:" << vol_id_;
        return false;
    }

    if(!reader_->init(cacheproxy_)){
        LOG_ERROR << "init journal writer failed,vol_id:" << vol_id_;
        return false;
    }
   
    if (!replayer_->init(vol_id_, vol_path_, 
                         string("localhost:50051"),
                         idproxy_, cacheproxy_, snapshotproxy_)){
        LOG_ERROR << "init journal replayer failed,vol_id:" << vol_id_;
        return false;
    }

    return true;
}

void Volume::fini()
{
    replayer_->deinit();
    writer_->deinit();
    reader_->deinit();
    pre_processor_->deinit();
    connection_->deinit();
    
    if (buffer_pool_ != NULL){
        nedalloc::neddestroypool(buffer_pool_);
        buffer_pool_ = NULL;
    }
}

shared_ptr<SnapshotProxy>& Volume::get_snapshot_proxy() const
{
    return snapshotproxy_;
}

JournalWriter& Volume::get_writer() const
{
    return *(writer_.get());
}

void Volume::start()
{
   /*start network receive*/
    connection_->start();
}

void Volume::stop()
{
   /*stop network receive*/
    connection_->stop();
}

}
