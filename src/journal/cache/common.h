#ifndef _COMMON_H
#define _COMMON_H

#include <unistd.h>
#include <string>
#include <memory>
#include "../message.hpp"
#include <boost/thread/shared_mutex.hpp>

using namespace std;
using namespace Journal;

/*boost read write lock */
typedef boost::shared_mutex       Mutex;
typedef boost::unique_lock<Mutex> WriteLock;
typedef boost::shared_lock<Mutex> ReadLock;

class CEntry
{
    const static uint8_t IN_MEM = 0;
    const static uint8_t IN_LOG = 1;
public:
    CEntry(){}
    explicit CEntry(uint64_t seq, string file, off_t offset);
    explicit CEntry(uint64_t seq, string file, off_t offset, shared_ptr<ReplayEntry> entry);
    CEntry(const CEntry& other);
    CEntry(CEntry&& other);
    CEntry& operator=(const CEntry& other);
    CEntry& operator=(CEntry&& other);
    ~CEntry(){}

    const uint64_t get_log_seq()const{
        return log_seq;
    }

    const string get_log_file()const{
        return log_file;
    }

    const off_t get_log_offset()const{
        return log_offset;
    }

    const uint8_t get_cache_type()const{
        return cache_type;
    }
    const shared_ptr<ReplayEntry> get_log_entry()const{
        return log_entry;
    }
    
    /*the centry take how many memory space*/
    size_t get_mem_size()const;

private:
    uint64_t log_seq;       //io log sequence number
    string   log_file;      //log file name  
    off_t    log_offset;    //log entry append to log offset 

    int      cache_type;    //cache in memory or on log file 
    shared_ptr<ReplayEntry> log_entry; //log entry in memory

    friend class Bcache; 
    friend class Jcache;
};

#endif
