#ifndef _COMMON_H
#define _COMMON_H

#include <unistd.h>
#include <string>
#include <memory>
#include "../nedmalloc.h"
#include "../replay_entry.hpp"
#include "../seq_generator.hpp"
#include <boost/thread/shared_mutex.hpp>

using namespace std;
using namespace Journal;

/*boost read write lock */
typedef boost::shared_mutex       Mutex;
typedef boost::unique_lock<Mutex> WriteLock;
typedef boost::shared_lock<Mutex> ReadLock;

/*cache item */
class CEntry
{
public:
    const static uint8_t IN_MEM = 0;
    const static uint8_t IN_JOURANL = 1;
public:
    CEntry(){}
    /*in journal */
    explicit CEntry(IoVersion seq, off_t bdev_off, size_t bdev_len, 
                    string jfile, off_t jfile_off);
    /*in memory*/
    explicit CEntry(IoVersion seq, off_t bdev_off, size_t bdev_len,
                    string jfile, off_t jfile_off, 
                    shared_ptr<ReplayEntry> entry);

    CEntry(const CEntry& other);
    CEntry(CEntry&& other);
    CEntry& operator=(const CEntry& other);
    CEntry& operator=(CEntry&& other);
    ~CEntry(){}

    IoVersion get_io_seq()const{
        return io_seq;
    }

    off_t get_blk_off()const{
        return blk_off;
    }

    size_t get_blk_len()const{
        return blk_len;
    }

    uint8_t get_cache_type()const{
        return cache_type;
    }
    
    void set_cache_type(uint8_t type){
        cache_type = type; 
    }

    string get_journal_file()const{
        return journal_file;
    }

    off_t get_journal_off()const{
        return journal_off;
    }

    shared_ptr<ReplayEntry>& get_journal_entry(){
        return journal_entry;
    }
    
    /*the centry take how many memory space*/
    size_t get_mem_size()const;

private:
    IoVersion io_seq;
    off_t     blk_off;
    size_t    blk_len; 

    /*in memory or on journal file*/
    int      cache_type;

    /*in journal entry*/
    string    journal_file;
    off_t     journal_off;
    /*in memory entry*/
    shared_ptr<ReplayEntry> journal_entry; 

    friend class Bcache; 
    friend class Jcache;
};

/*interface to genernate hash code when route*/
class IHashcode
{
public:
    IHashcode() = default; 
    virtual ~IHashcode(){}
    virtual int hashcode() = 0;
};

/*interface to read file*/
class IReadFile : public IHashcode
{
public:
    static int fid;
public:
    IReadFile() = default;
    IReadFile(string file, off_t pos, bool eos);
    virtual ~IReadFile(){}

    virtual int  open() = 0;
    virtual void close() = 0;
    virtual size_t read(off_t off, char* buf, size_t count) = 0;
    virtual size_t write(off_t off, char* buf, size_t count) = 0;

    /*call by cache or replayer*/
    size_t read_entry(off_t off, 
                      nedalloc::nedpool* bufpool,
                      shared_ptr<ReplayEntry>& entry);
    /*call by writer*/
    size_t write_entry(off_t off, 
                       shared_ptr<ReplayEntry>& entry);

public:
    string m_file;  /*file name*/ 
    int    m_fd;    /*file descriptor*/
    size_t m_size;  /*file size*/
    off_t  m_pos;   /*valid data offset*/
    bool   m_eos;   /*end of stream, true: can not get journal file from drserver*/
};

/*use synchronize read or pread */
class SyncReadFile: public IReadFile
{
public:
    SyncReadFile(string file, off_t pos, bool eos);
    virtual ~SyncReadFile();
   
    virtual int open() override;
    virtual void close() override;
    virtual size_t read(off_t off, char* buf, size_t count) override;
    virtual size_t write(off_t off, char* buf, size_t count) override;
    virtual int hashcode() override;
};

/*use mmap read*/
class MmapReadFile: public IReadFile
{
public:
    MmapReadFile(string file, off_t pos, bool eos);
    virtual ~MmapReadFile();

    virtual int open() override;
    virtual void close() override;
    virtual size_t read(off_t off, char* buf, size_t count) override;
    virtual size_t write(off_t off, char* buf, size_t count) override;
    virtual int hashcode() override;
private:
    void* m_mmap_base{nullptr};
}; 

/*interface use to route or dispatch*/
class IRoute
{
public:
    IRoute()= default;
    virtual ~IRoute(){} 

    virtual uint32_t route(int hashcode, uint32_t partion_num) = 0;
};

/*simple hash route implementation*/
class HashRoute: public IRoute
{
public:
    HashRoute() = default;
    virtual ~HashRoute(){}

    virtual uint32_t route(int hashcode, uint32_t partion_num){
        return hashcode % partion_num;
    }
};

#endif
