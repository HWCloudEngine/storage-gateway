#ifndef JOURNAL_READER_HPP
#define JOURNAL_READER_HPP

#include <memory>
#include <thread>

#include <boost/noncopyable.hpp>
#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>

#include "../common/blocking_queue.h"

#include "message.hpp"
#include "cache/cache_proxy.h"

using namespace std;

namespace Journal{

class JournalReader : private boost::noncopyable
{
public:
    explicit JournalReader(BlockingQueue<struct IOHookRequest>& read_queue, 
                           BlockingQueue<struct IOHookReply*>&  reply_queue); 
    virtual ~JournalReader();

    bool init(shared_ptr<CacheProxy> cacheproxy);
    bool deinit();
    
    void work();

private:
    /*in queue*/
    BlockingQueue<struct IOHookRequest>& m_read_queue; 
    /*out queue*/
    BlockingQueue<struct IOHookReply*>&  m_reply_queue; 

    shared_ptr<CacheProxy>  m_cacheproxy;  

    bool               m_run;
    shared_ptr<thread> m_thread;
};

}

#endif


