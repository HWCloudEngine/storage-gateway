#include "jcache.h"

bool operator<(const Jkey& a, const Jkey& b)
{
    return a.m_seq < b.m_seq;
}

ostream& operator<<(ostream& cout, const Jkey& key)
{
    cout <<"[seq:" << key.m_seq << "]";
    return cout;
}

bool Jcache::add(Jkey key, shared_ptr<CEntry> value)
{
    /*write lock*/
    WriteLock write(m_mutex);
    std::pair<jcache_itor_t, bool> ret;
    ret = m_jcache.insert(std::pair<Jkey, shared_ptr<CEntry>>(key, value));
    if(ret.second == false){
        cout << "[jcache] add key: " << key << "failed key existed" << endl;
        return false;  
    }

    cout << "[jcache] add key: " << key << "ok" << endl;
    return true;
}

shared_ptr<CEntry> Jcache::get(Jkey key)
{
    /*read lock */
    auto it = m_jcache.find(key);
    if(it != m_jcache.end()){
        return it->second;
    }

    cout << "[jcache] get key: " << key << "failed key not existed" << endl;
    return nullptr;
}

bool Jcache::update(Jkey key, shared_ptr<CEntry> value)
{
    /*write lock*/
    WriteLock write_lock(m_mutex);
    auto it = m_jcache.find(key);
    if(it != m_jcache.end()){
        m_jcache.erase(it);
    }
    std::pair<jcache_itor_t, bool> ret;
    ret = m_jcache.insert(std::pair<Jkey, shared_ptr<CEntry>>(key, value));
    if(ret.second == false){
        cout << "[jcache] update key: " << key << "failed key existed" << endl;
        return false;  
    }

    return true;
}

bool Jcache::del(Jkey key)
{
    WriteLock write_lock(m_mutex);
    auto it = m_jcache.find(key);
    if(it != m_jcache.end()){
        m_jcache.erase(it);
    }
    return true;
}

Jkey Jcache::top()
{
    auto it = m_jcache.begin();
    return it->first;
}

void Jcache::trace()
{
    cout << "[jcache] item_num:" << m_jcache.size() << endl;
    for(auto it : m_jcache)
    {
        cout <<"(" << it.first.m_seq << ")" \
             <<"[" << it.second.use_count() << "]" << endl;
    }
}
