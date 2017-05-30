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
#ifndef SRC_SG_CLIENT_VOLUME_H_
#define SRC_SG_CLIENT_VOLUME_H_
#include <string>
#include <atomic>
#include <boost/asio.hpp>
#include "nedmalloc.h"
#include "common/journal_entry.h"
#include "common/config_option.h"
#include "common/blocking_queue.h"
#include "common/volume_attr.h"
#include "common/ceph_s3_lease.h"
#include "rpc/common.pb.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"
#include "client_socket.h"
#include "journal_preprocessor.h"
#include "journal_writer.h"
#include "journal_reader.h"
#include "journal_replayer.h"
#include "snapshot/snapshot_proxy.h"
#include "backup/backup_decorator.h"
#include "backup/backup_proxy.h"

#define BUFFER_POOL_SIZE 1024*1024*64
#define REQUEST_BODY_SIZE 512

class VolumeManager;

class Volume {
 public:
    explicit Volume(VolumeManager& vol_manager,
                    const VolumeInfo& vol_info,
                    shared_ptr<CephS3LeaseClient> lease_client,
                    std::shared_ptr<WriterClient> writer_rpc_client,
                    int epoll_fd);
    Volume(const Volume& other) = delete;
    Volume& operator=(const Volume& other) = delete;
    virtual ~Volume();

    bool init();
    bool init_socket(raw_socket_t client_sock);
    bool deinit_socket();
    void fini();
    void start();
    void stop();

    const std::string get_vol_id();
    shared_ptr<JournalWriter> get_writer()const;
    shared_ptr<SnapshotProxy>& get_snapshot_proxy()const;
    shared_ptr<BackupProxy>&   get_backup_proxy()const;
    shared_ptr<ReplicateProxy>& get_replicate_proxy()const;

    // update volume attribute
    void update_volume_attr(const VolumeInfo& info);

 private:
    VolumeManager& vol_manager_;
    VolumeAttr vol_attr_;
    shared_ptr<CephS3LeaseClient> lease_client_;
    shared_ptr<WriterClient>  writer_rpc_client_;
    /*socket*/
    mutable raw_socket_t raw_socket_;
    // epoll fd to sync producer marker
    int epoll_fd_;
    /*memory pool for receive io hook data from raw socket*/
    nedalloc::nedpool* buffer_pool_;
    /*queues*/
    BlockingQueue<shared_ptr<JournalEntry>> entry_queue_;
    BlockingQueue<io_reply_t*> reply_queue_;
    BlockingQueue<shared_ptr<JournalEntry>> write_queue_;
    BlockingQueue<io_request_t> read_queue_;
    /*cache */
    shared_ptr<IDGenerator> idproxy_;
    shared_ptr<CacheProxy>  cacheproxy_;
    /*snapshot relevant*/
    mutable shared_ptr<SnapshotProxy>   snapshotproxy_;
    mutable shared_ptr<BackupDecorator> backupdecorator_;
    /*backup relevant*/
    mutable shared_ptr<BackupProxy> backupproxy_;
    /*replicate proxy */
    mutable shared_ptr<ReplicateProxy> rep_proxy_;
    /*network*/
    shared_ptr<ClientSocket> client_socket_;
    /*io preprocess*/
    shared_ptr<JournalPreProcessor>  pre_processor_;
    /*io write*/
    mutable shared_ptr<JournalWriter> writer_;
    /*io read*/
    shared_ptr<JournalReader> reader_;
    /*replay*/
    shared_ptr<JournalReplayer> replayer_;
};

#endif  // SRC_SG_CLIENT_VOLUME_H_
