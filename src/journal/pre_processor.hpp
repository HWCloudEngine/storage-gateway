#ifndef JOURNAL_PREPROCESSOR_HPP
#define JOURNAL_PREPROCESSOR_HPP
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>  
#include <boost/thread/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

#include "message.hpp"
#include "replay_entry.hpp"
#include "../common/config_parser.h"

using namespace std;

namespace Journal{

struct PrePreocessorConf{
    int thread_num;
    checksum_type_t checksum_type;
};

class PreProcessor
    :private boost::noncopyable
{
public:
    explicit PreProcessor(entry_queue& write_queue,
                                  entry_queue& entry_queue,
                                  std::condition_variable& recieve_cv,
                                  std::condition_variable& write_cv);
    virtual ~PreProcessor();
    void work();
    bool init(nedalloc::nedpool* buffer_pool,shared_ptr<ConfigParser> conf);
    bool deinit();
    bool cal_checksum(ReplayEntry * entry,bool sse_flag,checksum_type_t checksum_type);
private:
    entry_queue& write_queue_;
    entry_queue& entry_queue_;
    boost::thread_group worker_threads;
    nedalloc::nedpool* buffer_pool_;
    std::mutex mtx_;
    std::condition_variable& recieve_cv_;
    std::condition_variable& write_cv_;

    bool running_flag;
    struct PrePreocessorConf config;

};

}

#endif
