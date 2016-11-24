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

#include "../common/config_parser.h"
#include "../common/blocking_queue.h"
#include "message.hpp"
#include "journal_entry.hpp"

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
    explicit PreProcessor(BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                          BlockingQueue<shared_ptr<JournalEntry>>& write_queue);
    virtual ~PreProcessor();

    void work();
    bool init(std::shared_ptr<ConfigParser> conf);
    bool deinit();

private:
    /*input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*output queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
   
    /*config*/
    struct PrePreocessorConf config;

    /*crc and io merge thread group*/
    bool running_flag;
    boost::thread_group worker_threads;
};

}

#endif
