#ifndef JOURNAL_WRITER_HPP
#define JOURNAL_WRITER_HPP

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <map>
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

#include "../common/blocking_queue.h"
#include "../common/prqueue.h"
#include "../common/config_parser.h"

#include "seq_generator.hpp"
#include "cache/cache_proxy.h"
#include "../snapshot/snapshot_proxy.h"

#include "message.hpp"
#include "journal_entry.hpp"
#include "../rpc/clients/writer_client.hpp"
#include "../dr_server/ceph_s3_lease.h"

namespace Journal{

struct JournalWriterConf{
    uint64_t journal_max_size;
    std::string journal_mnt;
    int_least64_t write_timeout;
    int32_t version;
    checksum_type_t checksum_type;
    int32_t journal_limit;
};

class JournalWriter :private boost::noncopyable
{
public:
    explicit JournalWriter(std::string rpc_addr,
                           PRQueue<shared_ptr<JournalEntry>>&  write_queue,
                           BlockingQueue<struct IOHookReply*>& reply_queue);
    virtual ~JournalWriter();
    void work();
    bool init(std::string& vol, 
              std::shared_ptr<ConfigParser> conf,
              std::shared_ptr<IDGenerator> id_proxy, 
              std::shared_ptr<CacheProxy> cacheproxy,
              std::shared_ptr<SnapshotProxy> snapshotproxy,
              std::shared_ptr<CephS3LeaseClient> lease_client);
    bool deinit();
    //The following two function must be called in another thread,can't call in write thread
    //The write thread and another thread are single consumer/single producer module,communicate with lockfree queue
    bool get_writeable_journals(const std::string& uuid,const int limit);
    bool seal_journals(const std::string& uuid);

private:
    bool open_journal(uint64_t entry_size);
    bool get_journal();
    int64_t get_file_size(const char *path);
    bool write_journal_header();
    void send_reply(JournalEntry* entry,bool success);

    void handle_lease_invalid(std::string* journal_ptr);

    /*lease with dr server*/
    shared_ptr<CephS3LeaseClient> lease_client_;
    
    /*input queue*/
    PRQueue<shared_ptr<JournalEntry>>& write_queue_;
    /*output queue*/
    BlockingQueue<struct IOHookReply*>& reply_queue_;
    
    /*cache*/
    shared_ptr<IDGenerator> idproxy_;
    shared_ptr<CacheProxy> cacheproxy_;

    /*snapshot*/
    shared_ptr<SnapshotProxy> snapshot_proxy_;

    /*journal file prefetch and seal thread*/
    std::mutex rpc_mtx_;
    WriterClient rpc_client;
    boost::shared_ptr<boost::thread> thread_ptr;
    boost::lockfree::queue<std::string*> journal_queue;
    boost::lockfree::queue<std::string*> seal_queue;
    
    /*current operation journal file info*/
    FILE* cur_file_ptr;
    std::string *cur_journal;
    uint64_t cur_journal_size;
    
    std::atomic_int journal_queue_size;
    std::string vol_id;
    
    struct JournalWriterConf config;
    bool running_flag;
};

}

#endif


