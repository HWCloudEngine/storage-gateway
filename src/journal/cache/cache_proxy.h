#ifndef _PROXY_H
#define _PROXY_H

#include <unistd.h>
#include <string>
#include <memory>
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
        delete bcache;
        delete jcache;
    }
  
    /*journal writer or replayer add cache*/
    void write(string    log_file,         //log file name 
               off_t     log_off,          //log offset in log file  
               shared_ptr<ReplayEntry> log_entry);  //log entry

    /*io hook read handle*/
    int  read(off_t  off,   //offset in original block device
              size_t len,   //read data size 
              char*  buf);  //read data will store in buf
    
    /*journal replayer call*/
    shared_ptr<CEntry> pop();                                     
    /*journal replayer reclaim CEntry from jcache and bcache */
    bool reclaim(shared_ptr<CEntry> entry);         
    
    bool isfull(size_t cur_io_size);

    /*bcache full, evict the old item */
    void bcache_evict();
    
    /*debug*/
    Bcache* get_bcache()const{
        return bcache;
    }
    Jcache* get_jcache()const{
        return jcache; 
    }
    void trace();

private:
    string   blkdev;                ///block device 
    shared_ptr<IDGenerator> idproc; //id generator 

    size_t   total_mem_size;

    Jcache*  jcache;                //accelerate journal replay
    Bcache*  bcache;                //io read cache
};

#endif
