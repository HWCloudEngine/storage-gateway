#ifndef JOURNAL_PREPROCESSOR_HPP
#define JOURNAL_PREPROCESSOR_HPP

#include <mutex>
#include <condition_variable>

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>  
#include <boost/thread/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

#include "message.hpp"
#include "replay_entry.hpp"

namespace Journal{

class PreProcessor
    :private boost::noncopyable
{
public:
    explicit PreProcessor(boost::asio::ip::tcp::socket& socket_,
                                  entry_queue& write_queue,
                                  entry_queue& entry_queue,
                                  std::condition_variable& recieve_cv,
                                  std::condition_variable& write_cv);
    virtual ~PreProcessor();
    void work();
    bool init(nedalloc::nedpool* buffer_pool,int thread_num);
    bool deinit();
    bool cal_checksum(ReplayEntry * entry,bool sse_flag,checksum_type_t checksum_type);
private:
    entry_queue& write_queue_;
    entry_queue& entry_queue_;
    boost::thread_group worker_threads;
    nedalloc::nedpool* buffer_pool_;
    boost::asio::ip::tcp::socket& raw_socket_;
    std::mutex mtx_;
    std::condition_variable& recieve_cv_;
    std::condition_variable& write_cv_;

};

}

#endif