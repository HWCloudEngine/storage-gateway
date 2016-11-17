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

#include "../../log/log.h"
#include "../nedmalloc.h"
#include "../../common/blocking_queue.h"
#include "../../common/log_header.h"
#include "../../rpc/consumer.pb.h"
#include "../../rpc/clients/replayer_client.hpp"
#include "../seq_generator.hpp"
#include "../message.hpp"

#include "common.h"
#include "cache_proxy.h"

using namespace std;
using huawei::proto::JournalMarker;
using Journal::ReplayEntry;

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
    
    void init(nedalloc::nedpool* bufpool){
        m_buffer_pool = bufpool; 
    }

    /*start worker*/
    virtual void start() = 0;
    /*stop worker*/
    virtual void stop()  = 0;
    /*worker thread main loop*/
    virtual void loop()  = 0;
    /*push item to queue of the worker*/
    virtual void enqueue(void* item) = 0;
    
protected:
    atomic_bool           m_run{false};
    nedalloc::nedpool*    m_buffer_pool{nullptr}; 
    thread*               m_thread{nullptr};
    /*producer of the worker*/
    vector<IWorker*>      m_producer;
    /*consumer of the worker*/
    vector<IWorker*>      m_consumer;
    /*router use in the worker*/
    IRoute*               m_router{nullptr};
};

/*worker: read journal file and dispatch item to DestWorker*/
class ProcessWorker: public IWorker
{
public:
    ProcessWorker() = default;
    ProcessWorker(shared_ptr<IDGenerator> id_maker, 
                  shared_ptr<CacheProxy> cache_proxy)
        :m_id_generator(id_maker), m_cache_proxy(cache_proxy){}
    virtual ~ProcessWorker(){}

    void start() override;
    void stop()  override;
    void loop()  override;
   
    void enqueue(void* item);

private:
    /*read file and generate item to */
    void process_file(IReadFile* file);

private:
    BlockingQueue<IReadFile*>*  m_que;
    shared_ptr<IDGenerator> m_id_generator;
    shared_ptr<CacheProxy>  m_cache_proxy;
};

/*worker: get journal file from drserver by grpc*/
class SrcWorker: public IWorker
{
public:
    SrcWorker() = default;
    SrcWorker(string vol, 
              shared_ptr<ReplayerClient> rpc_cli, 
              shared_ptr<JournalMarker> lastest_mark)
                :m_volume(vol), m_grpc_client(rpc_cli), 
                 m_latest_marker(lastest_mark){}
    virtual ~SrcWorker(){}
    
    void start() override;
    void stop() override;

    void loop() override;

    void enqueue(void* item) override;

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
                  shared_ptr<IDGenerator> id_maker,
                  shared_ptr<CacheProxy> cache_proxy){
        m_volume       = volume;
        m_grpc_client  = rpc_cli;

        m_last_marker  = make_shared<JournalMarker>();
        m_latest_marker = make_shared<JournalMarker>();

        m_id_generator = id_maker;
        m_cache_proxy  = cache_proxy;
        LOG_DEBUG << "CacheRecovery create";
        init();
    }

    ~CacheRecovery(){
        LOG_DEBUG << "CacheRecovery create";
        fini(); 
    }
    
    void init(){
        const int recover_mpool_size = 100*1024*1024;
        m_buffer_pool = nedalloc::nedcreatepool(recover_mpool_size, 1);
        if(nullptr == m_buffer_pool){
            LOG_ERROR << "cache recovery init memory pool failed";
        }
    }
    
    void fini(){
        if(m_buffer_pool){
            nedalloc::neddestroypool(m_buffer_pool);
        }
    }

    /*start cache recover*/
    void start();
    /*wait until cache recover finished*/
    void stop();

private:
    string                     m_volume;               //volume name 
    nedalloc::nedpool*         m_buffer_pool{nullptr}; //memory pool
    shared_ptr<ReplayerClient> m_grpc_client;          //grpc client

    shared_ptr<JournalMarker>  m_last_marker;    //last marker
    shared_ptr<JournalMarker>  m_latest_marker;  //latest marker

    shared_ptr<IDGenerator>    m_id_generator;   //id generator 
    shared_ptr<CacheProxy>     m_cache_proxy;    //cache proxy 
        
    /*
     ****************************************************************
     *                          |----processor----|                 *
     * drserver--grpc-->src-----|----processor----|----->cache      *
     *                          |----processor----|                 *
     ****************************************************************
     */

    SrcWorker*     m_src_worker{nullptr}; //get journal files from drserver
    ProcessWorker* m_processor{nullptr};  //read centry from file, add to cache
    int            m_processor_num{3};    //processer concurrency
};

#endif
