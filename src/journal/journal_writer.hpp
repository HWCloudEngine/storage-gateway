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
#include "../rpc/clients/writer_client.hpp"

namespace Journal{

class JournalWriter
    :private boost::noncopyable
{
public:
    explicit JournalWriter(std::string rpc_addr,
                                 entry_queue& write_queue,
                                 boost::asio::ip::tcp::socket& raw_socket,
                                 std::mutex& mtx,
                                 std::condition_variable& cv);
    virtual ~JournalWriter();
    void work();
    bool init(std::string& vol);
    bool deinit();
    bool get_writeable_journals(const std::string& uuid,const int limit);
    bool seal_journals(const std::string& uuid, const std::list<std::string>& list_);
private:
    bool get_journal(uint64_t entry_size);
    int64_t get_file_size(const char *path);

    std::mutex& mtx_;
    std::condition_variable& cv_;
    WriterClient rpc_client;
    boost::shared_ptr<boost::thread> thread_ptr;
    entry_queue& write_queue_;
    boost::asio::ip::tcp::socket& raw_socket_;
    std::list<std::string> journals;
    FILE* file_ptr;
    
    std::string vol_id;
    uint64_t journal_max_size;
    uint64_t write_seq;
    double timeout;
};

}

#endif


