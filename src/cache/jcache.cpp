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

bool Jcache::get(Jkey key, shared_ptr<CEntry>& value)
{
    auto it = m_jcache.find(key);
    if(it != m_jcache.end()){
        value = it->second;
        return true;
    }
    return false;
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
