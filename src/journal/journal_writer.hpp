#ifndef JOURNAL_WRITER_HPP
#define JOURNAL_WRITER_HPP

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <condition_variable>
#include <chrono>

#include <time.h>
#include <atomic> 

#include <sys/stat.h>

#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/array.hpp>
#include <boost/asio.hpp>

#include "message.hpp"
#include "replay_entry.hpp"
#include "../rpc/clients/writer_client.hpp"
#include "../common/config_parser.h"


#include "seq_generator.hpp"
#include "cache/cache_proxy.h"

namespace Journal{

struct JournalWriterConf{
    uint64_t journal_max_size;
    std::string journal_mnt;
    double write_timeout;
    int32_t version;
    checksum_type_t checksum_type;
};

class JournalWriter
    :private boost::noncopyable
{
public:
    explicit JournalWriter(std::string rpc_addr,
                           entry_queue& write_queue,std::condition_variable& cv,
                           reply_queue& rep_queue,std::condition_variable& reply_cv);
    virtual ~JournalWriter();
    void work();
    bool init(std::string& vol, 
              ConfigParser& conf,
              shared_ptr<IDGenerator> id_proxy, 
              shared_ptr<CacheProxy> cacheproxy);
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
    void send_reply(ReplayEntry* entry,bool success);

    shared_ptr<IDGenerator> idproxy_;
    shared_ptr<CacheProxy> cacheproxy_;

    std::mutex mtx_;
    std::condition_variable& cv_;
    std::condition_variable& reply_cv_;
    WriterClient rpc_client;
    boost::shared_ptr<boost::thread> thread_ptr;
    entry_queue& write_queue_;
    reply_queue& reply_queue_;
    boost::lockfree::queue<std::string*> journal_queue;
    boost::lockfree::queue<std::string*> seal_queue;

    FILE* cur_file_ptr;
    std::string *cur_journal;
    uint64_t cur_journal_size;
    uint64_t write_seq;
    std::atomic_int journal_queue_size;
    std::string vol_id;
    
    struct JournalWriterConf config;

    bool running_flag;
};

}

#endif


