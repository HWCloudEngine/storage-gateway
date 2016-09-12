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
    explicit JournalReader(boost::asio::ip::tcp::socket& socket,
                           shared_ptr<CacheProxy> cacheproxy,
                           BlockingQueue<struct IOHookRequest>& read_queue);
    virtual ~JournalReader();

    bool init();
    bool deinit();
    
    /*add io read request*/
    bool push(struct IOHookRequest req);

    void work();

private:
    /*scoekt send callbcak*/
    void send_reply_cbt(struct IOHookReply* rsp,
                        const boost::system::error_code& error);
private:
    boost::asio::ip::tcp::socket& m_socket; 
    shared_ptr<CacheProxy>  m_cacheproxy;  
    nedalloc::nedpool*      m_mem_pool;    

    BlockingQueue<struct IOHookRequest>& m_read_queue;

    bool               m_run;
    shared_ptr<thread> m_thread;
};

}

#endif


