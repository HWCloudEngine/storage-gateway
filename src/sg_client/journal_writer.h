#ifndef JOURNAL_WRITER_H
#define JOURNAL_WRITER_H

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <condition_variable>
#include <chrono>
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
#include "common/config.h"
#include "common/journal_entry.h"
#include "common/ceph_s3_lease.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"
#include "snapshot/snapshot_proxy.h"
#include "rpc/clients/writer_client.h"
#include "message.h"
#include "rpc/clients/writer_client.h"
#include "epoll_event.h"

using namespace std;

namespace Journal{

class JournalWriter :private boost::noncopyable
{
public:
    explicit JournalWriter(BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                           BlockingQueue<struct IOHookReply*>& reply_queue);
    virtual ~JournalWriter();
    void work();
    bool init(const Configure& conf,
              string vol, 
              shared_ptr<IDGenerator> id_proxy, 
              shared_ptr<CacheProxy> cacheproxy,
              shared_ptr<SnapshotProxy> snapshotproxy,
              shared_ptr<CephS3LeaseClient> lease_client,
              shared_ptr<WriterClient> writer_client,
              int _epoll_fd);
    bool deinit();
    //The following two function must be called in another thread,can't call in write thread
    //The write thread and another thread are single consumer/single producer module,communicate with lockfree queue
    bool get_writeable_journals(const std::string& uuid,const int limit);
    bool seal_journals(const std::string& uuid);

    const string get_vol_id() const;
    // producer marker related methods
    void clear_producer_event();
    void hold_producer_marker();
    void unhold_producer_marker();
    bool is_producer_marker_holding();
    JournalMarker get_cur_producer_marker();
private:
    int get_next_journal();
    int open_current_journal();
    int to_seal_current_journal();
    int close_current_journal_file();
    void invalid_current_journal();

    int64_t get_file_size(const char *path);
    bool write_journal_header();
    void send_reply(JournalEntry* entry,bool success);

    void handle_lease_invalid();
    
    Configure conf_;
    /*lease with dr server*/
    shared_ptr<CephS3LeaseClient> lease_client_;
    
    /*input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
    /*output queue*/
    BlockingQueue<struct IOHookReply*>& reply_queue_;
    
    /*cache*/
    shared_ptr<IDGenerator> idproxy_;
    shared_ptr<CacheProxy> cacheproxy_;

    /*snapshot*/
    shared_ptr<SnapshotProxy> snapshot_proxy_;

    /*journal file prefetch and seal thread*/
    std::mutex rpc_mtx_;
    shared_ptr<WriterClient> rpc_client;
    boost::shared_ptr<boost::thread> thread_ptr;
    std::queue<std::pair<std::string, std::string>> journal_queue;
    std::queue<std::pair<std::string, std::string>> seal_queue;
    std::recursive_mutex journal_mtx_;
    std::mutex seal_mtx_;
    
    /*current operation journal file info*/
    FILE* cur_file_ptr;
    std::pair<std::string, std::string> cur_lease_journal;
    uint64_t cur_journal_size;
    
    std::string vol_id;
    
    bool running_flag;

    // new written size in journals since last update of producer marker
    uint64_t written_size_since_last_update;
    EpollEvent producer_event;
    // whether to hold updating producer marker
    std::atomic<bool> producer_marker_hold_flag;
    // the producer marker which need update
    JournalMarker cur_producer_marker;
    // mutex for producer marker
    std::mutex producer_mtx;
    // epoll fd, created in VolumeMgr, which collects the writers' events
    int epoll_fd;
};

}

#endif


