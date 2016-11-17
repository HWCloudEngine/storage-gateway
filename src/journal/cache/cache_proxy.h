#ifndef _PROXY_H
#define _PROXY_H

#include <unistd.h>
#include <string>
#include <atomic>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "common.h"
#include "jcache.h"
#include "bcache.h"
#include "../nedmalloc.h"
#include "../seq_generator.hpp"
#include "../../log/log.h"
using namespace std;

class CacheProxy
{
    const static size_t MAX_CACHE_LIMIT  = (100 * 1024 * 1024); //100MBytes
    const static size_t CACHE_EVICT_SIZE = (10 * 1024 * 1024);  //10MBytes
public:
    CacheProxy(){}
    explicit CacheProxy(string blk_dev, shared_ptr<IDGenerator> id_maker){
        LOG_INFO << "CacheProxy create";
        blkdev = blk_dev;
        idproc = id_maker;
        total_mem_size = 0;

        start_cache_evict_thr();

        jcache = new Jcache(blk_dev);
        bcache = new Bcache(blk_dev);
    } 

    /*each volume own a cproxy, disable copy constructor and operator=*/
    CacheProxy(const CacheProxy& other) = delete;
    CacheProxy(CacheProxy&& other) = delete;
    CacheProxy& operator=(const CacheProxy& other) = delete;
    CacheProxy& operator=(CacheProxy&& other) = delete;

    ~CacheProxy(){
        LOG_INFO << "CacheProxy destroy";
        stop_cache_evict_thr();
        delete bcache;
        delete jcache;
    }
  
    /*journal writer or replayer add cache*/
    void write(string journal_file, off_t  journal_off,
               shared_ptr<ReplayEntry> journal_entry);

    /*io hook read handle*/
    int  read(off_t  off, size_t len, char*  buf); 
    
    /*journal replayer call*/
    shared_ptr<CEntry> pop();                                     

    /*journal replayer reclaim CEntry from jcache and bcache */
    bool reclaim(shared_ptr<CEntry> entry);         
    
    /*check whether cache is full*/
    bool isfull(size_t cur_io_size);

    /*backgroud cache evict */
    void start_cache_evict_thr();
    void trigger_cache_evict();
    void cache_evict_work();
    void stop_cache_evict_thr();

    /*debug*/
    Bcache* get_bcache()const{
        return bcache;
    }
    Jcache* get_jcache()const{
        return jcache; 
    }
    void trace();

private:
    /*block device*/
    string   blkdev;
    /*io seq generator*/
    shared_ptr<IDGenerator> idproc;

    /*current cache memory size*/
    atomic<size_t>  total_mem_size; 
    
    /*backgroud cache evict*/
    bool               evict_run;
    thread*            evict_thread;
    mutex              evict_lock;
    condition_variable evict_cond;

    /*accelerate journal replay*/
    Jcache*  jcache;
    /*accelerate read cache*/
    Bcache*  bcache;
};

#endif
