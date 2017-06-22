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
#include <stdlib.h>
#include <list>
#include <functional>
#include "cache_recover.h"
#include "common/journal_entry.h"

void IWorker::register_consumer(IWorker* worker) {
    m_consumer.push_back(worker);
}

void IWorker::register_producer(IWorker* worker) {
    m_producer.push_back(worker);
}

ProcessWorker::ProcessWorker(shared_ptr<IDGenerator> id_maker,
                             shared_ptr<CacheProxy> cache_proxy)
           :m_id_generator(id_maker), m_cache_proxy(cache_proxy) {
    m_que = nullptr;
}

void ProcessWorker::start() {
    m_router = new HashRoute();
    m_que    = new BlockingQueue<JournalElement>();
    m_run    = true;
    m_thread = new thread(bind(&ProcessWorker::loop, this));
}

void ProcessWorker::stop() {
    if (m_thread) {
        m_thread->join();
        delete m_thread;
    }
    if (m_que) {
        delete m_que;
    }
    if (m_router) {
        delete m_router;
    }
    m_consumer.clear();
}

void ProcessWorker::loop() {
    while (m_run) {
        JournalElement file;
        bool ret = m_que->pop(file);
        if (!ret) {
            break;
        }
        if (!file.journal().empty()) {
            /*normal, read each file, and add to cache*/
            process_file(file);
            LOG_INFO << "process worker doing";
        } else {
            /*no more file , process exit*/
            m_run = false;
            LOG_INFO << "process worker done";
        }
    }
}

void ProcessWorker::enqueue(void* item) {
    m_que->push(*reinterpret_cast<JournalElement*>(item));
}

void ProcessWorker::process_file(const JournalElement& file) {
    /*prepare id generator, add file*/
    std::string path = g_option.journal_mount_point + file.journal();
    m_id_generator->add_file(path);
    
    unique_ptr<AccessFile> access_file;
    Env::instance()->create_access_file(path, false, &access_file);
    size_t access_file_size =  Env::instance()->file_size(path);

    LOG_INFO << "process file:" << path << " start_pos:"  << file.start_offset()
             << " end_pos:" << file.end_offset() << " size:" << access_file_size;

    off_t start = file.start_offset();
    //important: end should be fiile size, instead of end_offset
    //let parse to check where should stop parse entry
    off_t end = access_file_size;
    while (start < end) {
        shared_ptr<JournalEntry> journal_entry = std::make_shared<JournalEntry>();
        ssize_t ret = journal_entry->parse(&access_file, access_file_size, start);
        if (ret == -1) {
            LOG_ERROR << "process file read entry failed";
            break;
        }
        m_cache_proxy->write(path, start, journal_entry);
        start = ret;
    }

    m_id_generator->del_file(path);
}

SrcWorker::SrcWorker(std::string vol, shared_ptr<ReplayerClient> rpc_cli)
        : m_volume(vol), m_grpc_client(rpc_cli) {
}

void SrcWorker::start() {
    m_router = new HashRoute();
    if (m_consumer.size() > 0) {
        m_run = true;
        m_thread = new thread(bind(&SrcWorker::loop, this));
    }
}

void SrcWorker::stop() {
    if (m_thread) {
        m_thread->join();
        delete m_thread;
    }
    m_consumer.clear();
    if (m_router) {
        delete m_router;
    }
}

void SrcWorker::broadcast_consumer_exit() {
    for (auto it : m_consumer) {
        JournalElement file;
        file.set_journal("");
        it->enqueue(&file);
    }
}

void SrcWorker::loop() {
    bool ret = m_grpc_client->GetJournalMarker(m_volume, m_latest_marker);
    if (!ret) {
        LOG_ERROR << "src worker get journal marker failed";
        broadcast_consumer_exit();
        return;
    }

    while (m_run) {
        /*get journal file list from drserver*/
        const int limit = 10;
        list<JournalElement> journal_list;
        ret = m_grpc_client->GetJournalList(m_volume, m_latest_marker, limit,
                                            journal_list);
        if (!ret || journal_list.empty()) {
            LOG_ERROR << "src worker get journal list failed";
            m_run = false;
            broadcast_consumer_exit();
            break;
        }
        /*dispatch file to process work*/
        for (auto it : journal_list) {
            std::string path = g_option.journal_mount_point + it.journal();
            off_t start_pos = it.start_offset() == 0 ? sizeof(journal_file_header_t) : it.start_offset();
            it.set_start_offset(start_pos);
            std::hash<std::string> hash_fn;
            size_t hash = hash_fn(path);
            int cidx = m_router->route(hash, m_consumer.size());
            m_consumer[cidx]->enqueue(&it);
        }

        if (limit == journal_list.size()) {
            /*more journal file again, renew latest marker*/
            auto rit = journal_list.rbegin();
            m_latest_marker.set_cur_journal(rit->journal());
            m_latest_marker.set_pos(rit->end_offset());
            LOG_INFO << "src worker replay doing";
        } else {
            /*notify next chain that here no file any more*/
            m_run = false;
            broadcast_consumer_exit();
            LOG_INFO << "src worker replay done";
            break;
        }
    }
}

void SrcWorker::enqueue(void* item) {
    return;
}

CacheRecovery::CacheRecovery(std::string volume,
                             shared_ptr<ReplayerClient> rpc_cli,
                             shared_ptr<IDGenerator> id_maker,
                             shared_ptr<CacheProxy> cache_proxy)
    : m_volume(volume), m_grpc_client(rpc_cli), m_id_generator(id_maker),
    m_cache_proxy(cache_proxy) {
}

void CacheRecovery::start() {
    LOG_INFO << "cache recover start";
    m_processor = new ProcessWorker[m_processor_num];
    for (int i = 0; i < m_processor_num; i++) {
        /*todo: here use a trick*/
        new (&m_processor[i]) ProcessWorker(m_id_generator, m_cache_proxy);
        m_processor[i].start();
    }
    m_src_worker = new SrcWorker(m_volume, m_grpc_client);
    for (int i = 0; i < m_processor_num; i++) {
        m_src_worker->register_consumer(&m_processor[i]);
        m_processor[i].register_producer(m_src_worker);
    }
    m_src_worker->start();
    LOG_INFO << "cache recover start ok";
}

void CacheRecovery::stop() {
    LOG_INFO << "cache recover stop";
    if (m_src_worker) {
        m_src_worker->stop();
        delete m_src_worker;
    }
    if (m_processor) {
        for (int i = 0; i < m_processor_num; i++) {
            m_processor[i].stop();
        }
        delete [] m_processor;
    }
    LOG_INFO << "cache recover stop ok";
}
