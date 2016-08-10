#include "jcache.h"

bool operator<(const Jkey& a, const Jkey& b)
{
    return a.m_seq < b.m_seq;
}

bool Jcache::add(Jkey key, shared_ptr<CEntry> value)
{
    std::pair<jcache_itor_t, bool> ret;
    ret = m_jcache.insert(std::pair<Jkey, shared_ptr<CEntry>>(key, value));
    if(ret.second == false){
        return false;  
    }
    return true;
}

shared_ptr<CEntry> Jcache::get(Jkey key)
{
    auto it = m_jcache.find(key);
    if(it != m_jcache.end()){
        return it->second;
    }
    return nullptr;
}

bool Jcache::update(Jkey key, shared_ptr<CEntry> value)
{
    auto it = m_jcache.find(key);
    if(it != m_jcache.end()){
        m_jcache.erase(it);
    }
    return add(key, value);
}

bool Jcache::del(Jkey key)
{
    m_jcache.erase(key);
    return true;
}

Jkey Jcache::top()
{
    auto it = m_jcache.begin();
    return it->first;
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
