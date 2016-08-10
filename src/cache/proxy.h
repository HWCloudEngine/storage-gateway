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
    
    /*journal write call*/
    void write(uint64_t log_seq,                    //log sequence 
               string   log_file,                   //log file name 
               off_t    log_off,                    //log entry will append to log offset in log file  
               shared_ptr<log_header_t> log_entry); //log entry

    /*io hook read handle*/
    int  read(off_t  off,   //offset in original block device
              size_t len,   //read data size 
              char*  buf);  //read data will store in buf
    
    /*journal replayer call*/
    bool pop(shared_ptr<CEntry>& item);  //replayer get item and replay it
    bool free(shared_ptr<CEntry>& item); //replayer finish item and free it from both jcache and bcahe

    /*debug*/
    void trace();

private:
    string   blkdev;
    Jcache*  jcache; //accelerate journal replay
    Bcache*  bcache; //io read cache
};

#endif
