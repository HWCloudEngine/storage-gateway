#ifndef _BCACHE_H
#define _BCACHE_H
#include "unistd.h"
#include <string>
#include <memory>
#include <map>
#include "common.h"

using namespace std;

/*key in bcache*/
class Bkey
{
public:
    Bkey(){}
    Bkey(off_t off, size_t len, uint64_t seq)
        :m_off(off),m_len(len),m_seq(seq){
    }
    Bkey(const Bkey& other){
        m_off = other.m_off;
        m_len = other.m_len;
        m_seq = other.m_seq;
    }
    Bkey(Bkey&& other){
        m_off = std::move(other.m_off);
        m_len = std::move(other.m_len);
        m_seq = std::move(other.m_seq);
    }

    Bkey& operator=(const Bkey& other){
        if(this != &other){
            m_off = other.m_off;
            m_len = other.m_len;
            m_seq = other.m_seq;
        } 
        return *this;
    }
    Bkey& operator=(Bkey&& other){
        if(this != &other){
            m_off = std::move(other.m_off);
            m_len = std::move(other.m_len);
            m_seq = std::move(other.m_seq);
        } 
        return *this;
    }

    friend bool operator<(const Bkey& a, const Bkey& b);

    off_t    m_off; //io offset
    size_t   m_len; //io length
    uint64_t m_seq; //io sequence
};


/*block read cache in memory*/
class Bcache
{
public:
    Bcache(){}
    /*blk_dev:  orignal block device path
     *mem_limit: max memory cache 
     */
    Bcache(string bdev, size_t mem_limit)
        :m_blkdev(bdev), m_mem_limit(mem_limit), m_mem_size(0){}
    
    Bcache(const Bcache& other) = delete;
    Bcache(Bcache&& other) = delete;
    Bcache& operator=(const Bcache& other) = delete;
    Bcache& operator=(Bcache&& other) = delete;
    
    ~Bcache(){}
    
    int read(off_t off, size_t len, char* buf);
    
    /*CRUD op*/
    bool add(Bkey key, shared_ptr<CEntry> value){
        std::pair<std::map<Bkey, shared_ptr<CEntry>>::iterator, bool> ret;
        ret = m_bcache.insert(std::pair<Bkey, shared_ptr<CEntry>>(key, value));
        if(ret.second == false){
           return false;  
        }
        return true;
    }

    bool get(Bkey key, shared_ptr<CEntry>& value){
        auto it = m_bcache.find(key);
        if(it != m_bcache.end()){
            value = it->second;
            return true;
        }
        return false;
    }

    bool update(Bkey key, shared_ptr<CEntry> value){
        auto it = m_bcache.find(key);
        if(it != m_bcache.end()){
            m_bcache.erase(it);
        }
        return add(key, value);
    }

    bool del(Bkey key){
        m_bcache.erase(key);
        return true;
    }

    /*check memory full or not, if full, CEntry point to log file, else in memory*/
    bool isfull(int io_size){
        if(m_mem_size+io_size > m_mem_limit)
            return true;
        return false;
    }
    
    /*debug*/
    void trace();

private:
    string                         m_blkdev;    //original block device
    Lock                           m_lock;      //read write lock
    map<Bkey, shared_ptr<CEntry>>  m_bcache;    //memory cache
    size_t                         m_mem_limit; //memory cache max capacity
    size_t                         m_mem_size;  //current memory cache size
}; 

#endif
