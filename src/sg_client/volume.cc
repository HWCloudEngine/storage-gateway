#include <boost/bind.hpp>
#include <algorithm>
#include "log/log.h"
#include "volume_manager.h"
#include "volume.h"

namespace Journal{

Volume::Volume(VolumeManager& vol_manager, const Configure& conf, 
               const VolumeInfo& vol_info, shared_ptr<CephS3LeaseClient> lease_client, 
               shared_ptr<WriterClient> writer_rpc_client, int epoll_fd, 
               raw_socket_t client_sock)
              : vol_manager_(vol_manager), conf_(conf), vol_attr_(vol_info), 
                lease_client_(lease_client),
                writer_rpc_client_(writer_rpc_client), epoll_fd_(epoll_fd), 
                raw_socket_(client_sock) 
{
}

Volume::~Volume()
{
    LOG_INFO << "volume destroy ";
    fini();
    LOG_INFO << "volume destroy ok";
}

bool Volume::init()                  
{
    buffer_pool_ = nedalloc::nedcreatepool(BUFFER_POOL_SIZE, 2);
    if(buffer_pool_ == NULL){
        LOG_ERROR << "create buffer pool failed";
        return false;
    }

    idproxy_.reset(new IDGenerator());
    cacheproxy_.reset(new CacheProxy(vol_attr_.blk_device(), idproxy_,conf_));
    snapshotproxy_.reset(new SnapshotProxy(conf_, vol_attr_, entry_queue_)); 

    backupdecorator_.reset(new BackupDecorator(vol_attr_.vol_name(), snapshotproxy_));
    backupproxy_.reset(new BackupProxy(conf_, vol_attr_, backupdecorator_));

    connection_.reset(new Connection(vol_manager_, raw_socket_, entry_queue_, read_queue_, reply_queue_));
    rep_proxy_.reset(new ReplicateProxy(conf_, vol_attr_.vol_name(), vol_attr_.vol_size(),snapshotproxy_));

    pre_processor_.reset(new JournalPreProcessor(entry_queue_, write_queue_));
    reader_.reset(new JournalReader(read_queue_, reply_queue_));
    writer_.reset(new JournalWriter(write_queue_, reply_queue_,vol_attr_));
    replayer_.reset(new JournalReplayer(vol_attr_));

    if(!connection_->init(buffer_pool_)){
        LOG_ERROR << "init connection failed,vol_name:" << vol_attr_.vol_name();
        return false;
    }

    if(!pre_processor_->init(conf_)){
        LOG_ERROR << "init pre_processor failed,vol_name:"<< vol_attr_.vol_name();
        return false;
    }
    
    /*todo read from config*/
    if(!writer_->init(conf_, 
                      idproxy_, cacheproxy_, snapshotproxy_, 
                      lease_client_,writer_rpc_client_,epoll_fd_)){
        LOG_ERROR << "init journal writer failed,vol_name:" << vol_attr_.vol_name();
        return false;
    }

    if(!reader_->init(cacheproxy_)){
        LOG_ERROR << "init journal writer failed,vol_name:" << vol_attr_.vol_name();
        return false;
    }
   
    if (!replayer_->init(conf_,idproxy_, cacheproxy_,
                snapshotproxy_,rep_proxy_)){
        LOG_ERROR << "init journal replayer failed,vol_name:" << vol_attr_.vol_name();
        return false;
    }

    return true;
}

void Volume::fini()
{
    LOG_INFO << "Volume fini" ;
    connection_->deinit();
    LOG_INFO << "Volume fini connection deinit" ;
    reader_->deinit();
    LOG_INFO << "Volume fini reader deinit" ;
    pre_processor_->deinit();
    LOG_INFO << "Volume fini processor deinit" ;
    writer_->deinit();
    LOG_INFO << "Volume fini writer deinit" ;
    replayer_->deinit();
    LOG_INFO << "Volume fini replayer deinit" ;
    if (buffer_pool_ != NULL){
        nedalloc::neddestroypool(buffer_pool_);
        buffer_pool_ = NULL;
    }
    LOG_INFO << "Volume fini ok" ;
}

shared_ptr<SnapshotProxy>& Volume::get_snapshot_proxy() const
{
    return snapshotproxy_;
}

shared_ptr<BackupProxy>& Volume::get_backup_proxy() const
{
    return backupproxy_;
}

shared_ptr<ReplicateProxy>& Volume::get_replicate_proxy() const
{
    return rep_proxy_;
}

shared_ptr<JournalWriter> Volume::get_writer() const
{
    return writer_;
}

void Volume::start()
{
   /*start network receive*/
    connection_->start();
}

void Volume::stop()
{
   /*stop network receive*/
    LOG_INFO << "volume stop ";
    connection_->stop();
    LOG_INFO << "volume stop ok";
}

const string Volume::get_vol_id() {
    return vol_attr_.vol_name();
}

void Volume::update_volume_attr(const VolumeInfo& info){
    vol_attr_.update(info);    
}

}
