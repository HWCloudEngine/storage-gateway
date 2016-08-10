#include "jcache.h"

bool operator<(const Jkey& a, const Jkey& b)
{
    return a.m_seq < b.m_seq;
}

bool Jcache::pop(shared_ptr<CEntry>& item)
{
    if(m_jcache.empty())
        return false;
    auto it = m_jcache.begin();
    item = it->second;
    return true;
}

void Jcache::trace()
{
    cout << "-----jcache-------" << endl;
    for(auto it : m_jcache)
    {
        cout <<"(" << it.first.m_seq << ")" \
             <<"[" << it.second.use_count() << "]" << endl;
    }
}
