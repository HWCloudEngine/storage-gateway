#include "jcache.h"

void Jcache::push(shared_ptr<CEntry> entry)
{
    return (void)m_queue.push(entry);
}

shared_ptr<CEntry> Jcache::pop()
{
    return m_queue.pop();
}

void Jcache::trace()
{
    for(int i=0; i < m_queue.entry_number(); i++){
        shared_ptr<CEntry> entry = m_queue[i];
        LOG_INFO  << " jcache item_num:" << m_queue.entry_number()
                  << " blk_off:" << entry->get_blk_off()
                  << " blk_len:" << entry->get_blk_len()
                  << " seq: "    << entry->get_io_seq();
    }
}
