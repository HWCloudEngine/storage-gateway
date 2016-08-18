#ifndef JOURNAL_WRITER_HPP
#define JOURNAL_WRITER_HPP

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <condition_variable>
#include <time.h>

#include <sys/stat.h>

#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>

#include "message.hpp"
#include "replay_entry.hpp"
#include "../rpc/clients/writer_client.hpp"

namespace Journal{

class JournalWriter
    :private boost::noncopyable
{
public:
    explicit JournalWriter(std::string rpc_addr,
                                 entry_queue& write_queue,
                                 boost::asio::ip::tcp::socket& raw_socket,
                                 std::condition_variable& cv);
    virtual ~JournalWriter();
    void work();
    bool init(std::string& vol);
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

    std::mutex mtx_;
    std::condition_variable& cv_;
    WriterClient rpc_client;
    boost::shared_ptr<boost::thread> thread_ptr;
    entry_queue& write_queue_;
    boost::asio::ip::tcp::socket& raw_socket_;
    boost::lockfree::queue<std::string*> journal_queue;
    boost::lockfree::queue<std::string*> seal_queue;

    FILE* cur_file_ptr;
    std::string *cur_journal;
    uint64_t cur_journal_size;
    uint64_t journal_max_size;
    
    std::string vol_id;
    uint64_t write_seq;
    double write_timeout;
    int32_t version;
    checksum_type_t checksum_type;
};

}

#endif


