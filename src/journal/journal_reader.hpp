#ifndef JOURNAL_READER_HPP
#define JOURNAL_READER_HPP

#include <memory>
#include <thread>

#include <boost/noncopyable.hpp>
#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>

#include "../common/blocking_queue.h"

#include "replay_entry.hpp"
#include "message.hpp"
#include "cache/cache_proxy.h"

using namespace std;

namespace Journal{

class JournalReader : private boost::noncopyable
{
public:
    explicit JournalReader(reply_queue& reply_queue,
                           condition_variable& reply_queue_cv,
                           BlockingQueue<struct IOHookRequest>& read_queue); 
    virtual ~JournalReader();

    bool init(shared_ptr<CacheProxy> cacheproxy);
    bool deinit();
    
    void work();

private:
    reply_queue&            m_reply_queue;     /*reply queue*/
    condition_variable&     m_reply_queue_cv;  /*reply queue condition variable*/

    BlockingQueue<struct IOHookRequest>& m_read_queue; /*requst queue*/

    shared_ptr<CacheProxy>  m_cacheproxy;  

    bool               m_run;
    shared_ptr<thread> m_thread;
};

}

#endif


