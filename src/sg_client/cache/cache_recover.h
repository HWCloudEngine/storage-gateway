/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    backup_proxy.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup entry
*  
*************************************************/
#ifndef SRC_SG_CLIENT_CACHE_CACHE_RECOVER_H_
#define SRC_SG_CLIENT_CACHE_CACHE_RECOVER_H_
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
#include <vector>
#include <functional>
#include "log/log.h"
#include "common/blocking_queue.h"
#include "rpc/consumer.pb.h"
#include "rpc/clients/replayer_client.h"
#include "../nedmalloc.h"
#include "../seq_generator.h"
#include "../message.h"
#include "common.h"
#include "cache_proxy.h"

using namespace std;
using huawei::proto::JournalMarker;

/*interface to all kind of worker thread*/
class IWorker {
 public:
    IWorker(){}
    virtual ~IWorker() = default;

    void register_consumer(IWorker* worker);
    void register_producer(IWorker* worker);
    /*start worker*/
    virtual void start() = 0;
    /*stop worker*/
    virtual void stop()  = 0;
    /*worker thread main loop*/
    virtual void loop()  = 0;
    /*push item to queue of the worker*/
    virtual void enqueue(void* item) = 0;

 protected:
    /*the work*/
    atomic_bool m_run{false};
    thread* m_thread{nullptr};
    /*producer of the worker*/
    vector<IWorker*> m_producer;
    /*consumer of the worker*/
    vector<IWorker*> m_consumer;
    /*router use in the worker*/
    IRoute* m_router{nullptr};
};

/*worker: read journal file and add to cache*/
class ProcessWorker: public IWorker {
 public:
    ProcessWorker() = default;
    ProcessWorker(shared_ptr<IDGenerator> id_maker,
                  shared_ptr<CacheProxy>  cache_proxy);
    virtual ~ProcessWorker() = default;

    void start() override;
    void stop()  override;
    void loop()  override;
    void enqueue(void* item) override;

 private:
    /*read file and generate item to */
    void process_file(const JournalElement& file);

 private:
    BlockingQueue<JournalElement>* m_que;
    shared_ptr<IDGenerator> m_id_generator;
    shared_ptr<CacheProxy>  m_cache_proxy;
};

/*worker: get journal file from drserver by grpc*/
class SrcWorker: public IWorker {
 public:
    SrcWorker() = default;
    SrcWorker(std::string vol, shared_ptr<ReplayerClient> rpc_cli);
    virtual ~SrcWorker() = default;

    void start() override;
    void stop() override;
    void loop() override;
    void enqueue(void* item) override;

 private:
    /*notify consumer producer not provide element any more*/
    void broadcast_consumer_exit();

 private:
    std::string m_volume;
    shared_ptr<ReplayerClient> m_grpc_client;
    JournalMarker  m_latest_marker;
};

/*Cache Reovery entry*/
class CacheRecovery {
 public:
    CacheRecovery() = default;
    CacheRecovery(std::string volume,
                  shared_ptr<ReplayerClient> rpc_cli,
                  shared_ptr<IDGenerator> id_maker,
                  shared_ptr<CacheProxy> cache_proxy);
    ~CacheRecovery() = default;

    /*start cache recover*/
    void start();
    /*wait until cache recover finished*/
    void stop();

 private:
    std::string  m_volume;
    shared_ptr<ReplayerClient> m_grpc_client;
    shared_ptr<IDGenerator> m_id_generator;
    shared_ptr<CacheProxy>  m_cache_proxy;

    /*
     ****************************************************************
     *                          |----processor----|                 *
     * drserver--grpc-->src-----|----processor----|----->cache      *
     *                          |----processor----|                 *
     ****************************************************************
     */

    /*thread get journal file from drserver*/
    SrcWorker* m_src_worker{nullptr};
    /*thread read entry from file and add to cache*/
    ProcessWorker* m_processor{nullptr};
    /*process worker concurrence*/
    int m_processor_num{3};
};

#endif  // SRC_SG_CLIENT_CACHE_CACHE_RECOVER_H_
