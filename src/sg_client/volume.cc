/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    volume.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  volume content
*
*************************************************/
#include <algorithm>
#include <boost/bind.hpp>
#include "log/log.h"
#include "volume_manager.h"
#include "volume.h"

Volume::Volume(VolumeManager& vol_manager, const VolumeInfo& vol_info,
               shared_ptr<CephS3LeaseClient> lease_client, int epoll_fd)
              : vol_manager_(vol_manager), vol_attr_(vol_info),
                lease_client_(lease_client), epoll_fd_(epoll_fd) {
}

Volume::~Volume() {
    fini();
}

bool Volume::init() {
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " init";
    idproxy_.reset(new IDGenerator());
    cacheproxy_.reset(new CacheProxy(vol_attr_.blk_device(), idproxy_));
    snapshotproxy_.reset(new SnapshotProxy(vol_attr_, entry_queue_));
    backupdecorator_.reset(new BackupDecorator(vol_attr_.vol_name(), snapshotproxy_));
    backupproxy_.reset(new BackupProxy(vol_attr_, backupdecorator_));
    rep_proxy_.reset(new ReplicateProxy(vol_attr_.vol_name(), vol_attr_.vol_size(), snapshotproxy_));
    pre_processor_.reset(new JournalPreProcessor(entry_queue_, write_queue_));
    reader_.reset(new JournalReader(read_queue_, reply_queue_));
    writer_.reset(new JournalWriter(write_queue_, reply_queue_, vol_attr_));
    replayer_.reset(new JournalReplayer(vol_attr_));
    if (!pre_processor_->init()) {
        LOG_ERROR << "init pre_processor failed,vol_name:"<< vol_attr_.vol_name();
    }
    if (!writer_->init(idproxy_, cacheproxy_, snapshotproxy_, lease_client_, epoll_fd_)) {
        LOG_ERROR << "init journal writer failed,vol_name:" << vol_attr_.vol_name();
    }
    if (!reader_->init(cacheproxy_)) {
        LOG_ERROR << "init journal writer failed,vol_name:" << vol_attr_.vol_name();
    }
    if (!replayer_->init(idproxy_, cacheproxy_, snapshotproxy_, rep_proxy_)) {
        LOG_ERROR << "init journal replayer failed,vol_name:" << vol_attr_.vol_name();
    }
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " init ok";
}

void Volume::fini() {
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " fini";
    if (client_socket_) {
        client_socket_->deinit();
        LOG_INFO << "volume fini connection deinit";
    }
    reader_->deinit();
    LOG_INFO << "volume fini reader deinit";
    pre_processor_->deinit();
    LOG_INFO << "volume fini processor deinit";
    writer_->deinit();
    LOG_INFO << "volume fini writer deinit";
    replayer_->deinit();
    LOG_INFO << "volume fini replayer deinit";
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " fini ok";
}

bool Volume::init_socket(raw_socket_t client_sock) {
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " init socket";
    raw_socket_ = client_sock;
    client_socket_.reset(new ClientSocket(vol_manager_, raw_socket_, entry_queue_,
                            write_queue_, read_queue_, reply_queue_));
    if (!client_socket_->init()) {
        LOG_ERROR << "init connection failed vol:" << vol_attr_.vol_name();
        return false;
    }
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " init socket ok";
    return true;
}

bool Volume::deinit_socket() {
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " deinit socket";
    if (client_socket_) {
        client_socket_->deinit();
        client_socket_.reset();
    }
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " deinit socket ok";
}

void Volume::start() {
    /*start network receive*/
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " start";
    client_socket_->start();
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " start ok";
}

void Volume::stop() {
    /*stop network receive*/
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " stop";
    if (client_socket_) {
        client_socket_->stop();
    }
    LOG_INFO << "volume:" << vol_attr_.vol_name() << " stop ok";
}

shared_ptr<SnapshotProxy>& Volume::get_snapshot_proxy() const {
    return snapshotproxy_;
}

shared_ptr<BackupProxy>& Volume::get_backup_proxy() const {
    return backupproxy_;
}

shared_ptr<ReplicateProxy>& Volume::get_replicate_proxy() const {
    return rep_proxy_;
}

shared_ptr<JournalWriter> Volume::get_writer() const {
    return writer_;
}

const string Volume::get_vol_id() {
    return vol_attr_.vol_name();
}

void Volume::update_volume_attr(const VolumeInfo& info) {
    vol_attr_.update(info);
}
