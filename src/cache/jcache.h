#ifndef _JCACHE_H
#define _JCACHE_H 

#include <string.h>
#include <map>
#include "common.h"
using namespace std;

class Jkey
{
public: 
    Jkey(uint64_t seq):m_seq(seq){}
    Jkey(const Jkey& other):m_seq(other.m_seq){}
    Jkey(Jkey&& other):m_seq(std::move(other.m_seq)){}

    Jkey& operator=(const Jkey& other)
    {
        if(this != &other)
            this->m_seq = other.m_seq;
        return *this;
    }
    
    friend bool operator<(const Jkey&a, const Jkey& b);

    uint64_t m_seq; //log sequence no
};

class Jcache
{
public:
    Jcache(){}
    Jcache(string blk_dev):m_blkdev(blk_dev){
        m_jcache.clear(); 
    }

    Jcache(const Jcache& other) = delete;
    Jcache(Jcache&& other) = delete;
    Jcache& operator=(const Jcache& other) = delete;
    Jcache& operator=(Jcache&& other) = delete;
    
    ~Jcache(){
        m_jcache.clear(); 
    }
    
    bool add(Jkey key, shared_ptr<CEntry> value){
        std::pair<std::map<Jkey, shared_ptr<CEntry>>::iterator, bool> ret;
        ret = m_jcache.insert(std::pair<Jkey, shared_ptr<CEntry>>(key, value));
        if(ret.second == false){
            return false;  
        }
        return true;
    }

    bool get(Jkey key, shared_ptr<CEntry>& value){
        auto it = m_jcache.find(key);
        if(it != m_jcache.end()){
            value = it->second;
            return true;
        }
        return false;
    }

    bool update(Jkey key, shared_ptr<CEntry> value){
        auto it = m_jcache.find(key);
        if(it != m_jcache.end()){
            m_jcache.erase(it);
        }
        return add(key, value);
    }

    bool del(Jkey key){
        m_jcache.erase(key);
        return true;
    }
    
    bool pop(shared_ptr<CEntry>& item);

    /*debug*/
    void trace();
private:
    string                        m_blkdev;  //original block device
    Lock                          m_lock;    //read write lock
    map<Jkey, shared_ptr<CEntry>> m_jcache;  //act as queue
};

#endif
