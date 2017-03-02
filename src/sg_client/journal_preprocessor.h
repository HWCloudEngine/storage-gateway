#ifndef JOURNAL_PREPROCESSOR_H
#define JOURNAL_PREPROCESSOR_H
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>  
#include <boost/thread/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include "common/config.h"
#include "common/blocking_queue.h"
#include "common/journal_entry.h"
#include "message.h"

using namespace std;

namespace Journal{

class JournalPreProcessor : private boost::noncopyable
{
public:
    explicit JournalPreProcessor(BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                          BlockingQueue<shared_ptr<JournalEntry>>& write_queue);
    virtual ~JournalPreProcessor();

    void work();
    bool init(const Configure& conf);
    bool deinit();

private:
    Configure conf_;

    /*input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*output queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
   
    /*crc and io merge thread group*/
    bool running_flag;
    boost::thread_group worker_threads;
};

}

#endif
