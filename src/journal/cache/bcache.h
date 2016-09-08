#ifndef _BCACHE_H
#define _BCACHE_H

#include "unistd.h"
#include "iostream"
#include <string>
#include <memory>
#include <map>
#include "common.h"
#include "../nedmalloc.h"
#include "../message.hpp"
#include "../seq_generator.hpp"
#include "../../log/log.h"

using namespace std;

/*key in bcache*/
class Bkey
{
public:
    Bkey() = default;
    Bkey(off_t off, size_t len, IoVersion seq)
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
    friend ostream& operator<<(ostream& cout, const Bkey& key);

    off_t     m_off; //io offset
    size_t    m_len; //io length
    IoVersion m_seq; //io sequence
};


struct BkeyCompare{
    bool operator()(const Bkey& a, const Bkey& b){
        return a < b;
    }
};
typedef map<Bkey, shared_ptr<CEntry>, BkeyCompare> bcache_map_t;
typedef map<Bkey, shared_ptr<CEntry>, BkeyCompare>::iterator bcache_itor_t;

/*block read cache in memory*/
class Bcache
{
public:
    Bcache() = default;
    /*blk_dev:  orignal block device path
     *mem_limit: max memory cache 
     */
    Bcache(string bdev, size_t mem_limit){
        LOG_INFO << "Bcache create";
        m_blkdev = bdev;
        m_mem_limit = mem_limit;
        m_mem_size = 0;
        m_item_limit = 0;
        m_item_num = 0;
        init();
    }

    Bcache(const Bcache& other) = delete;
    Bcache(Bcache&& other) = delete;
    Bcache& operator=(const Bcache& other) = delete;
    Bcache& operator=(Bcache&& other) = delete;
    
    ~Bcache(){
        LOG_INFO << "Bcache destroy";
        m_bcache.clear();
        m_mem_size = 0;
        m_item_limit = 0;
        m_item_num = 0;

        fini();
    }
   
    void init(){
        const int bcache_mpool_size = 10*1024*1024;
        m_buffer_pool = nedalloc::nedcreatepool(bcache_mpool_size, 1);
        if(nullptr == m_buffer_pool){
            LOG_ERROR << "Bcache init memory pool failed";
        }
    }

    void fini(){
        if(m_buffer_pool){
            nedalloc::neddestroypool(m_buffer_pool);
        }
    }

    int read(off_t off, size_t len, char* buf);
    
    /*CRUD op*/
    bool add(Bkey key, shared_ptr<CEntry> value);
    shared_ptr<CEntry> get(Bkey key);
    bool update(Bkey key, shared_ptr<CEntry> value);
    bool del(Bkey key);

    /*check memory full or not, if full CEntry point to log file,else in memory
     */
    bool isfull(int io_size);
   
    /*debug*/
    void trace();

private:
    bcache_itor_t _data_lower_bound(off_t offset, size_t length);

    void _find_hit_region(off_t offset, 
                          size_t length, 
                          bcache_map_t& region_hits);  //cache hit region
    
    void _find_miss_region(off_t  off,  size_t len,                   
                    const vector<Bkey>& merged_bkeys, //merged cache hit keys
                    vector<Bkey>& miss_bkeys);        //out: cache miss keys
    
    void _merge_hit_region(vector<Bkey> bkeys,          //hit keys
                           vector<Bkey>& merged_bkeys); //merge hit keys
   
    /*read from cache*/
    int _cache_hit_read(off_t off, size_t len, char* buf, 
                        const vector<Bkey>& hit_keys, 
                        bcache_map_t& hit_cache_snapshot);
    
    /*read from block*/
    int _cache_miss_read(off_t off, size_t len, char* buf, 
                        const vector<Bkey>& miss_keys);
    
private:
    string                        m_blkdev;       //original block device
    nedalloc::nedpool*            m_buffer_pool;  //memory pool

    Mutex                         m_mutex;     //read write lock
    bcache_map_t                  m_bcache;    //memory cache
    size_t                        m_mem_limit; //memory cache max capacity
    size_t                        m_mem_size;  //current memory cache size
    size_t                        m_item_limit; //map entry item limit(reserved)
    size_t                        m_item_num;   //map entry item num(reserved)
}; 

#endif
