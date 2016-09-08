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

using namespace std;

class CacheProxy
{
    const static int MAX_CACHE_LIMIT = (100 * 1024 * 1024); //100MBytes
public:
    CacheProxy(){}
    explicit CacheProxy(string blk_dev, shared_ptr<IDGenerator> id_maker){
        blkdev = blk_dev;
        idproc = id_maker;
        jcache = new Jcache(blk_dev);
        bcache = new Bcache(blk_dev, MAX_CACHE_LIMIT);
    } 
    /*each volume own a cproxy, disable copy constructor and operator=*/
    CacheProxy(const CacheProxy& other) = delete;
    CacheProxy(CacheProxy&& other) = delete;
    CacheProxy& operator=(const CacheProxy& other) = delete;
    CacheProxy& operator=(CacheProxy&& other) = delete;

    ~CacheProxy(){
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
    /*replayer get log cursor*/
    Jkey top();                                     
    /*replayer get CEntry from jcache*/
    shared_ptr<CEntry> retrieve(const Jkey& key);   
    /*replayer reclaim CEntry from jcache and bcache */
    bool reclaim(shared_ptr<CEntry> data);         
  
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
    Jcache*  jcache;                //accelerate journal replay
    Bcache*  bcache;                //io read cache
};

#endif
