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

    Jkey& operator=(const Jkey& other){
        if(this != &other)
            this->m_seq = other.m_seq;
        return *this;
    }
    
    friend bool operator<(const Jkey&a, const Jkey& b);

    uint64_t m_seq; //log sequence no
};

struct JkeyCompare{
    bool operator()(const Jkey& a, const Jkey& b){
        return a < b;
    }
};
typedef map<Jkey, shared_ptr<CEntry>, JkeyCompare> jcache_map_t;
typedef map<Jkey, shared_ptr<CEntry>, JkeyCompare>::iterator jcache_itor_t;

class Jcache {
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
    
    bool add(Jkey key, shared_ptr<CEntry> value);
    shared_ptr<CEntry> get(Jkey key);
    bool update(Jkey key, shared_ptr<CEntry> value);
    bool del(Jkey key);
    
    /*get the minimum Jkey in jcache*/
    Jkey top();

    /*debug*/
    void trace();

private:
    string                        m_blkdev;  //original block device
    Lock                          m_lock;    //read write lock
    jcache_map_t                  m_jcache;  //act as queue
};

#endif
