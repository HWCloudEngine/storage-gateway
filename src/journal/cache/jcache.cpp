#include "jcache.h"

void Jcache::push(shared_ptr<CEntry> entry)
{
    return  (void)m_queue.push(entry);
}

shared_ptr<CEntry> Jcache::pop()
{
    return m_queue.pop();
}

void Jcache::trace()
{
    LOG_INFO << "item_num:" << m_queue.entry_number();
}
