#include <boost/bind.hpp>
#include "journal_reader.hpp"
#include "../log/log.h"

namespace Journal{
    

JournalReader::JournalReader(boost::asio::ip::tcp::socket& socket,
                            shared_ptr<CacheProxy> cacheproxy,
                            BlockingQueue<struct IOHookRequest>& read_queue)
    :m_socket(socket),m_cacheproxy(cacheproxy),m_mem_pool(nullptr), 
     m_read_queue(read_queue),m_run(false)
{
    init();
}

JournalReader::~JournalReader()
{
    deinit();
}


bool JournalReader::init()
{
   const int BUFFER_POOL_SIZE = 1 * 1024 * 1024;
   m_mem_pool = nedalloc::nedcreatepool(BUFFER_POOL_SIZE,1);
   m_run = true;
   m_thread.reset(new thread(std::bind(&JournalReader::work, this)));
   return true;
}

bool JournalReader::deinit()
{
    m_run = false;
    m_thread->join();
    nedalloc::neddestroypool(m_mem_pool);
    return true;
}

void JournalReader::work()
{
    while(m_run)
    {
        struct IOHookRequest ioreq;
        ioreq = m_read_queue.pop();
        int iorsp_len = sizeof(struct IOHookReply) + ioreq.len;
        struct IOHookReply* iorsp = (struct IOHookReply*)
                                    nedalloc::nedpmalloc(m_mem_pool,iorsp_len);
        iorsp->magic    = ioreq.magic;
        iorsp->reserves = ioreq.reserves;
        iorsp->handle   = ioreq.handle;
        char* buf       = (char*)iorsp + sizeof(struct IOHookReply);
        cout << "[JournalReader] read"
            << " magic:" << ioreq.magic 
            << " id:"    << ioreq.reserves
            << " off:"   << ioreq.offset 
            << " len:"   << ioreq.len << endl;
        int ret = m_cacheproxy->read(ioreq.offset, ioreq.len, buf);
        assert(ret == 0);
        iorsp->error = 0;
        iorsp->len   = ioreq.len;

        boost::asio::async_write(m_socket,
                                 boost::asio::buffer((char*)iorsp, iorsp_len),
                                 boost::bind(&JournalReader::send_reply_cbt, 
                                             this, 
                                             iorsp,
                                             boost::asio::placeholders::error));
    }
}

void JournalReader::send_reply_cbt(struct IOHookReply* rsp,
                                   const boost::system::error_code& error)
{   
    if(rsp){
        cout << "[JournalReader] read send reply"
            << " magic:"  << rsp->magic 
            << " id:"     << rsp->reserves
            << " len:"    << rsp->len 
            << " status:" << (error ? "failed" : "ok")
            << endl;
        nedalloc::nedpfree(m_mem_pool, rsp); 
    }
}

}
