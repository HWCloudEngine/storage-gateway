/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_writer.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  handle writer io
*
*************************************************/
#ifndef SRC_SG_CLIENT_JOURNAL_WRITER_H_
#define SRC_SG_CLIENT_JOURNAL_WRITER_H_
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <string>
#include <utility> 
#include <map>
#include <queue>
#include <memory>
#include <time.h>
#include <atomic>
#include <sys/stat.h>
#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/lockfree/queue.hpp>
#include "common/blocking_queue.h"
#include "common/config_option.h"
#include "common/journal_entry.h"
#include "common/ceph_s3_lease.h"
#include "common/volume_attr.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"
#include "snapshot/snapshot_proxy.h"
#include "rpc/clients/writer_client.h"
#include "message.h"
#include "epoll_event.h"
#include "journal_marker.h"

class JournalWriter :private boost::noncopyable {
 public:
    explicit JournalWriter(BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                           BlockingQueue<io_reply_t*>& reply_queue,
                           VolumeAttr& vol_attr);
    virtual ~JournalWriter();
    void work();
    bool init(shared_ptr<IDGenerator> id_proxy,
              shared_ptr<CacheProxy> cacheproxy,
              shared_ptr<SnapshotProxy> snapshotproxy,
              shared_ptr<CephS3LeaseClient> lease_client,
              shared_ptr<WriterClient> writer_client,
              int _epoll_fd);
    bool deinit();
    // The following two function must be called in another thread,can't call in write thread
    // The write thread and another thread are single consumer/single producer module,communicate with lockfree queue
    bool get_writeable_journals(const std::string& uuid, const int limit);
    bool seal_journals(const std::string& uuid);

    VolumeAttr& get_vol_attr();
    MarkerHandler& get_maker_handler();

 private:
    inline int get_next_journal();
    inline int open_current_journal();
    inline int to_seal_current_journal();
    inline int close_current_journal_file();
    inline void invalid_current_journal();
    inline bool write_journal_header();
    inline void send_reply(JournalEntry* entry, bool success);
    inline void handle_lease_invalid();
    inline bool write_journal_preprocess();
    inline void write_journal_response(shared_ptr<JournalEntry> entry, bool success);
    inline void synchronize_snapshot_cmd(shared_ptr<JournalEntry> entry);
    inline void seal_snapshot_journal(shared_ptr<JournalEntry> entry);

    /*lease with dr server*/
    shared_ptr<CephS3LeaseClient> lease_client_;
    /*input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
    /*output queue*/
    BlockingQueue<io_reply_t*>& reply_queue_;
    /*cache*/
    shared_ptr<IDGenerator> idproxy_;
    shared_ptr<CacheProxy> cacheproxy_;
    /*snapshot*/
    shared_ptr<SnapshotProxy> snapshot_proxy_;
    /*journal file prefetch and seal thread*/
    std::mutex rpc_mtx_;
    shared_ptr<WriterClient> rpc_client;
    boost::shared_ptr<boost::thread> thread_ptr;
    std::queue<std::pair<std::string, JournalElement>> journal_queue;
    std::queue<std::pair<std::string, JournalElement>> seal_queue;
    std::recursive_mutex journal_mtx_;
    std::mutex seal_mtx_;
    /*current operation journal file info*/
    unique_ptr<AccessFile> cur_file_;
    std::pair<std::string, JournalElement> cur_lease_journal;
    uint64_t cur_journal_size;
    VolumeAttr& vol_attr_;
    bool running_flag;
    MarkerHandler marker_handler;
};
#endif  // SRC_SG_CLIENT_JOURNAL_WRITER_H_
