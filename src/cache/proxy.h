#ifndef _PROXY_H
#define _PROXY_H

#include <unistd.h>
#include <string>
#include <memory>
#include "common.h"
#include "jcache.h"
#include "bcache.h"

using namespace std;

class Cproxy
{
    const static int MAX_CACHE_LIMIT = (100 * 1024 * 1024); //100MBytes
public:
    Cproxy(){}
    explicit Cproxy(string blk_dev){
        blkdev = blk_dev;
        jcache = new Jcache(blk_dev);
        bcache = new Bcache(blk_dev, MAX_CACHE_LIMIT);
    }
    
    /*each volume own a cproxy, disable copy constructor and operator=*/
    Cproxy(const Cproxy& other) = delete;
    Cproxy(Cproxy&& other) = delete;
    Cproxy& operator=(const Cproxy& other) = delete;
    Cproxy& operator=(Cproxy&& other) = delete;

    ~Cproxy(){
        delete bcache;
        delete jcache;
    }
    
    /*journal writer or replayer add cache*/
    void write(uint64_t log_seq,                    //log sequence 
               string   log_file,                   //log file name 
               off_t    log_off,                    //log entry will append to log offset in log file  
               shared_ptr<ReplayEntry> log_entry);  //log entry

    /*io hook read handle*/
    int  read(off_t  off,   //offset in original block device
              size_t len,   //read data size 
              char*  buf);  //read data will store in buf
    
    /*journal replayer call*/
    Jkey top();                                     //replayer get log cursor
    shared_ptr<CEntry> retrieve(const Jkey& key);   //replayer get CEntry from jcache
    bool reclaim(shared_ptr<CEntry> data);          //replayer reclaim CEntry from jcache and bcache
  
    /*debug*/
    Bcache* get_bcache()const{
        return bcache;
    }
    Jcache* get_jcache()const{
        return jcache; 
    }
    void trace();

private:
    string   blkdev;
    Jcache*  jcache; //accelerate journal replay
    Bcache*  bcache; //io read cache
};

#endif
