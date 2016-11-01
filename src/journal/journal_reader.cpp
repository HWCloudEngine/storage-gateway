#include <boost/bind.hpp>
#include "journal_reader.hpp"
#include "../log/log.h"

namespace Journal{
    
JournalReader::JournalReader(reply_queue& reply_queue,
                             condition_variable& reply_queue_cv,
                             BlockingQueue<struct IOHookRequest>& read_queue)
    :m_reply_queue(reply_queue),
     m_reply_queue_cv(reply_queue_cv), 
     m_read_queue(read_queue), 
     m_run(false)
{
    LOG_INFO << "JournalReader create";
}

JournalReader::~JournalReader()
{
    LOG_INFO << "JournalReader destory";
}


bool JournalReader::init(shared_ptr<CacheProxy> cacheproxy)
{
   m_cacheproxy = cacheproxy;
   m_run = true;
   m_thread.reset(new thread(std::bind(&JournalReader::work, this)));
   return true;
}

bool JournalReader::deinit()
{
    m_run = false;
    m_thread->join();
    return true;
}

void JournalReader::work()
{
    while(m_run)
    {
        /*fetch io read request*/
        struct IOHookRequest ioreq;
        bool ret = m_read_queue.pop(ioreq);
        if(!ret){
            break;
        } 
        int iorsp_len = sizeof(struct IOHookReply) + ioreq.len;
        struct IOHookReply* iorsp = (struct IOHookReply*)new char[iorsp_len];
        iorsp->magic    = ioreq.magic;
        iorsp->reserves = ioreq.reserves;
        iorsp->handle   = ioreq.handle;
        char* buf       = (char*)iorsp + sizeof(struct IOHookReply);
        LOG_DEBUG << "read"
                  << " magic:" << ioreq.magic 
                  << " id:"    << ioreq.reserves
                  << " off:"   << ioreq.offset 
                  << " len:"   << ioreq.len;
        /*read*/
        int read_size = m_cacheproxy->read(ioreq.offset, ioreq.len, buf);
        iorsp->error = 0;
        iorsp->len   = ioreq.len;

        /*send reply*/
        if(!m_reply_queue.push(iorsp))
        {
            delete [] iorsp;
            return;
        }
        m_reply_queue_cv.notify_one();
   }
}

}
