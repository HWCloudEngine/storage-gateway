#include "log/log.h"
#include "jcache.h"

Jkey::Jkey(IoVersion seq)
{
    m_seq = seq; 
}

Jkey::Jkey(const Jkey& other)
{
    m_seq = other.m_seq;
}

Jkey::Jkey(Jkey&& other)
{
    m_seq = std::move(other.m_seq);
}

Jkey& Jkey::operator=(const Jkey& other)
{
    if(this != &other){
        m_seq = other.m_seq;
    } 
    return *this;
}

Jkey& Jkey:: operator=(Jkey&& other)
{
    if(this != &other){
        m_seq = std::move(other.m_seq);
    } 
    return *this;
}

bool operator<(const Jkey& a, const Jkey& b)
{
    return a.m_seq < b.m_seq;
}

ostream& operator<<(ostream& cout, const Jkey& key)
{
    LOG_INFO << "[" << " seq:" << key.m_seq << "]";
    return cout;
}

Jcache::Jcache(string blk_dev)
{
    m_blkdev = blk_dev;
    LOG_INFO << "Jcache create";
}

Jcache::~Jcache()
{
    LOG_INFO << "Jcache destroy";
}

bool Jcache::add(Jkey key, shared_ptr<CEntry> value)
{
    WriteLock write_lock(m_mutex);
    auto ret = m_cache.insert({key, value});
    if(ret.second == false){
        LOG_ERROR << "add jkey:" << key << "failed";
        return false;  
    }
    return true;
}

shared_ptr<CEntry> Jcache::get(Jkey key)
{
    ReadLock read_lock(m_mutex);
    auto it = m_cache.find(key);
    if(it != m_cache.end()){
        return it->second;
    }
    LOG_ERROR << "get jkey: " << key  << "failed";
    return nullptr;
}

bool Jcache::update(Jkey key, shared_ptr<CEntry> value)
{
    WriteLock write_lock(m_mutex);
    auto it = m_cache.find(key);
    if(it != m_cache.end()){
        m_cache.erase(it);
    }

    auto ret = m_cache.insert({key, value});
    if(ret.second == false){
        LOG_ERROR << "update jkey: " << key << "failed";
        return false;  
    }
    return true;
}

bool Jcache::del(Jkey key)
{
    WriteLock write_lock(m_mutex);
    auto it = m_cache.find(key);
    if(it != m_cache.end()){
        m_cache.erase(it);
        return true;
    }
    LOG_ERROR << "del jkey:" << key << "failed";
    return false;
}

void Jcache::push(shared_ptr<CEntry> entry)
{
    IoVersion ioseq = entry->get_io_seq();
    Jkey key(ioseq);
    bool ret = add(key, entry);
    if(!ret){
        ret = update(key, entry);
        if(!ret){
            LOG_ERROR << "jcache push key:" << key << "failed";
        }
    }
}

shared_ptr<CEntry> Jcache::pop()
{
    /*here has read lock, so put it in block*/
    {
        if(empty()){
            //LOG_ERROR << "jcache pop empty";
            return nullptr; 
        }
    }

    auto it = m_cache.begin();
    shared_ptr<CEntry> out = it->second;
    IoVersion ioseq = out->get_io_seq();
    Jkey key(ioseq);
    bool ret = del(key);
    if(!ret){
        LOG_ERROR << "jcache pop key:" << key << "failed";
    }

    return out;
}

int Jcache::size()const
{
    ReadLock read_lock(m_mutex);
    return m_cache.size();
}

bool Jcache::empty()const
{
    ReadLock read_lock(m_mutex);
    return m_cache.empty();
}

Jcache::Iterator::Iterator(map<Jkey, shared_ptr<CEntry>>::iterator it)
{
    m_it = it; 
}

bool Jcache::Iterator::operator==(const Iterator& other)
{
    return m_it == other.m_it; 
}

bool Jcache::Iterator::operator!=(const Iterator& other)
{
    return m_it != other.m_it; 
}

Jcache::Iterator& Jcache::Iterator::operator++()
{
    m_it++; 
    return *this;
}

Jcache::Iterator Jcache::Iterator::operator++(int)
{
    Jcache::Iterator prev(m_it);
    m_it++;
    return prev;
}

Jkey Jcache::Iterator::first()
{
    return m_it->first;
}

shared_ptr<CEntry> Jcache::Iterator::second()
{
    return m_it->second; 
}

Jcache::Iterator Jcache::begin()
{
    return Jcache::Iterator(m_cache.begin());
}

Jcache::Iterator Jcache::end()
{
    return Jcache::Iterator(m_cache.end());
}

void Jcache::trace()
{
    for(auto it : m_cache){
        LOG_INFO  << " jcache size:" << m_cache.size()
            << " blk_off:" << it.second->get_blk_off()
            << " blk_len:" << it.second->get_blk_len()
            << " seq: "    << it.second->get_io_seq();
    }
}
