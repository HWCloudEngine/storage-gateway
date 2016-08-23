#ifndef __CACHE_RECOVER_H
#define __CACHE_RECOVER_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <map>
#include <functional>

#include "../common/log_header.h"
#include "../rpc/consumer.pb.h"
#include "../rpc/clients/replayer_client.hpp"
#include "seq_generator.hpp"
#include "message.hpp"

using namespace std;
using cond = condition_variable;
using huawei::proto::JournalMarker;
using Journal::ReplayEntry;

/*interface use to genernate hash code when route*/
class IHashcode
{
public:
    IHashcode() = default; 
    virtual ~IHashcode(){}
    virtual int hashcode() = 0;
};

/*interface use to read file*/
class IReadFile : public IHashcode
{
public:
    static int fid;
public:
    IReadFile() = default;
    IReadFile(string file, off_t pos, bool eos);
    virtual ~IReadFile(){}

    int open();
    void close();
    virtual size_t read(off_t off, char* buf, size_t count) = 0;

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
    
    virtual size_t read(off_t off, char* buf, size_t count) override;
    virtual int hashcode() override;
};

/*use mmap read*/
class MmapReadFile: public IReadFile
{
public:
    MmapReadFile(string file, off_t pos);
    virtual ~MmapReadFile();
    
    virtual size_t read(off_t off, char* buf, size_t count) override;
    virtual int hashcode() override;
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

/*element read from journal file, use to add to cache*/
class ReplayElement:public IHashcode
{
public:
    ReplayElement(IoVersion seq, string file, off_t off,
                  shared_ptr<ReplayEntry> entry, bool eos){
        m_log_seq   = seq;
        m_log_file  = file;
        m_log_off   = off;
        m_log_entry = entry;
        m_eos = eos;
    }

    virtual ~ReplayElement(){}

    virtual int hashcode(){
        return 0;    
    }

    friend ostream& operator<<(ostream& out, const ReplayElement& re);

public:
    IoVersion m_log_seq;   /*io sequence*/
    string    m_log_file;  /*journal file name*/
    off_t     m_log_off;   /*journal file offset*/
    shared_ptr<ReplayEntry> m_log_entry; /*journal log entry*/
    bool      m_eos;       /*end of stream, true: journal file read over*/
};


/*interface to all kind of worker thread*/
class IWorker
{
public:
    IWorker(){}
    virtual ~IWorker(){}

    void register_consumer(IWorker* worker){
        m_consumer.push_back(worker);
    }
    
    void register_producer(IWorker* worker){
        m_producer.push_back(worker);
    }

    bool is_producer_over(){
        for(auto it : m_producer){
            if(!it->m_run)
                return false;
        } 
        return true;
    }
    
    /*start worker*/
    virtual void start() = 0;
    /*stop worker*/
    virtual void stop()  = 0;
    /*worker thread main loop*/
    virtual void loop()  = 0;
    /*push item to queue of the worker*/
    virtual void inqueue(void* item) = 0;
    
protected:
    atomic_bool           m_run{false};

    thread*               m_thread{nullptr};
    mutex*                m_mutex{nullptr};
    cond*                 m_cond{nullptr};
    
    /*producer of the worker*/
    vector<IWorker*>      m_producer;
    /*consumer of the worker*/
    vector<IWorker*>      m_consumer;
    /*router use in the worker*/
    IRoute*               m_router{nullptr};
};

/*worker: add cache entry to cache*/
class DestWorker: public IWorker
{
public:
    DestWorker() = default;
    //DestWorker(shared_ptr<CacheProxy> proxy)
    //    :m_cache_proxy(proxy){}
    virtual ~DestWorker(){}
    
    void start() override;
    void stop()  override;
    void loop()  override;
    void inqueue(void* item) override;

private:
    queue<ReplayElement*>* m_que;
    //shared_ptr<CacheProxy> m_cache_proxy;
};

/*worker: read journal file and dispatch item to DestWorker*/
class ProcessWorker: public IWorker
{
public:
    ProcessWorker() = default;
    ProcessWorker(shared_ptr<IDGenerator> id_maker)
        :m_id_generator(id_maker){}
    virtual ~ProcessWorker(){}

    void start() override;
    void stop()  override;
    void loop()  override;
   
    void inqueue(void* item);

private:
    /*calculate each io data size*/
    size_t cal_data_size(off_len_t* off_len, int count);
    /*read file and generate item to */
    void process_file(IReadFile* file);

private:
    queue<IReadFile*>*      m_que;
    shared_ptr<IDGenerator> m_id_generator;
};

/*worker: get journal file from drserver by grpc*/
class SrcWorker: public IWorker
{
public:
    SrcWorker() = default;
    SrcWorker(string vol, shared_ptr<ReplayerClient> rpc_cli, 
              shared_ptr<JournalMarker> lastest_mark)
        :m_volume(vol), m_grpc_client(rpc_cli), m_latest_marker(lastest_mark){}
    virtual ~SrcWorker(){}
    
    void start() override;
    void stop() override;

    void loop() override;

    void inqueue(void* item) override;

private:
    string m_volume;
    shared_ptr<ReplayerClient> m_grpc_client;
    shared_ptr<JournalMarker>  m_latest_marker;
};


/*Cache Reovery entry*/
class CacheRecovery
{
public:
    CacheRecovery() = default;
    CacheRecovery(string volume, 
                  shared_ptr<ReplayerClient> rpc_cli, 
                  shared_ptr<IDGenerator> id_maker){
        m_volume       = volume;
        m_grpc_client  = rpc_cli;
        m_id_generator = id_maker;
        m_last_marker  = make_shared<JournalMarker>();
        m_latest_marker = make_shared<JournalMarker>();
    }

    ~CacheRecovery(){}
    
    /*start cache recover*/
    void start();
    /*wait until cache recover finished*/
    void stop();

private:
    string                     m_volume;         //volume name 

    shared_ptr<ReplayerClient> m_grpc_client;    //grpc client
    shared_ptr<JournalMarker>  m_last_marker;    //last marker
    shared_ptr<JournalMarker>  m_latest_marker;  //latest marker

    shared_ptr<IDGenerator>    m_id_generator;   //id generator 
    //shared_ptr<CacheProxy>     m_cache_proxy;    //cache proxy 
        
    /*
     ****************************************************************
     *                          |----processor----|                 *
     * drserver--grpc-->src-----|----processor----|---dest--->cache *
     *                          |----processor----|                 *
     ****************************************************************
     */

    SrcWorker*     m_src_worker{nullptr};    //worker: get journal file from drserver
    ProcessWorker* m_processor{nullptr};     //worker: read journal file and generate log sequence
    int            m_processor_num{3};       //processer concurrency
    DestWorker*    m_dest_worker{nullptr};   //worker: add replayentry to cache
};

#endif
