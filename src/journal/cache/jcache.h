#ifndef _JCACHE_H
#define _JCACHE_H 
#include <iostream>
#include <string.h>
#include <map>
#include "common.h"
#include "../../common/blocking_queue.h"
#include "../../log/log.h"
using namespace std;

class Jcache {
public:
    Jcache(){}
    Jcache(string blk_dev):m_blkdev(blk_dev){
        LOG_INFO << "Jcache create";
    }

    Jcache(const Jcache& other) = delete;
    Jcache(Jcache&& other) = delete;
    Jcache& operator=(const Jcache& other) = delete;
    Jcache& operator=(Jcache&& other) = delete;
    
    ~Jcache(){
        LOG_INFO << "Jcache destroy";
    }

    void push(shared_ptr<CEntry> value);
    shared_ptr<CEntry> pop();
 
    int size()const{
        return m_queue.size();
    }

    bool empty()const{
        return m_queue.empty();
    }

    /*debug*/
    void trace();

private:
    string m_blkdev;  //original block device
    BlockingQueue<shared_ptr<CEntry>> m_queue;  // queue
};

#endif
