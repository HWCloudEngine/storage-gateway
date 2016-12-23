#include <stdlib.h>
#include <string.h>
#include "cache_recover.h"
#include "../journal_entry.hpp"

void ProcessWorker::start()
{
    m_router = new HashRoute();
    m_que    = new BlockingQueue<File*>();
    m_run    = true;
    m_thread = new thread(bind(&ProcessWorker::loop, this));
    LOG_INFO << "process worker start";
}
   
void ProcessWorker::stop()
{
    if(m_thread){
        m_thread->join();
        delete m_thread;
    }
    if(m_que){
        delete m_que;
    }
    if(m_router){
        delete m_router;
    }
    m_consumer.clear();
    LOG_INFO << "process worker stop";
}

void ProcessWorker::loop()
{
    while(m_run) {
        File* file = m_que->pop();
        if(!file->m_eos){
            /*normal, read each file, and add to cache*/
            process_file(file); 
        } else {
            /*no more file , process exit*/
            m_run = false;
            LOG_INFO << "process worker discharge out";
        }
        delete file; 
    }
}

void ProcessWorker::enqueue(void* item)
{
    m_que->push((File*)item);
}

void ProcessWorker::process_file(File* file)
{
    /*prepare id generator, add file*/
    m_id_generator->add_file(file->m_file);

    int ret = file->open(); 
    if(ret == -1){
        return;
    }

    LOG_INFO << "Process file:" << file->m_file << " pos:"  << file->m_pos
             << " size:" << file->m_size << " eos:"  << file->m_eos; 

    if(file->m_size > 0){
        off_t start = file->m_pos;
        off_t end   = file->m_size;
        while(start < end){
            string journal_file = file->m_file;
            off_t  journal_off  = start;
            shared_ptr<JournalEntry> journal_entry = nullptr; 
            ssize_t ret = file->read_entry(start, journal_entry);
            if(ret == -1){
                LOG_ERROR << "Process file read entry failed";
                break;
            }
            start += ret;
            m_cache_proxy->write(journal_file, journal_off, journal_entry);
        }
    }

    file->close();
    m_id_generator->del_file(file->m_file);
}

void SrcWorker::start()
{
    m_router = new HashRoute();
    if(m_consumer.size() > 0){
        m_run = true;
        m_thread = new thread(bind(&SrcWorker::loop, this));
    }
    LOG_INFO << "src worker start";
}

void SrcWorker::stop()
{
    if(m_thread){
        m_thread->join();
        delete m_thread;
    }
    m_consumer.clear();
    if(m_router){
        delete m_router;
    }
    LOG_INFO << "src worker stop";
}

void SrcWorker::broadcast_consumer_exit()
{
    for(auto it : m_consumer){
        File* file = new File("", -1, true);
        it->enqueue(file);
    }
}

void SrcWorker::loop()
{
    /*todo: 
     * dr server should modify, 
     * if no journal marker exist return false when GetJournalMarker
     * other than meaningless journal marker
     */
    bool ret = m_grpc_client->GetJournalMarker(m_volume, m_latest_marker); 
    if(!ret){
        LOG_ERROR << "src worker get journal marker failed";
        /*notify next chain(process worker) exit*/
        broadcast_consumer_exit();
        return;
    }
    
    /*dispatch the first journal*/
    string name = m_latest_marker.cur_journal();
    off_t  pos  = m_latest_marker.pos();
    File* file = new File(name, pos, false); 
    int cidx = m_router->route(file->fid, m_consumer.size());
    m_consumer[cidx]->enqueue(file);
    
    LOG_INFO << "src worker journamarker file: " << name << " pos:" << pos;
    while(m_run){
        /*get journal file list from drserver*/
        int limit = 10;
        list<string> journal_list; 
        bool ret = m_grpc_client->GetJournalList(m_volume, m_latest_marker, 
                                                limit, journal_list);
        if(!ret || journal_list.empty()){
            LOG_ERROR << "src worker get journal list failed";
            m_run = false;
            broadcast_consumer_exit();
            break;
        }
       
        /*dispatch file to next chain*/
        for(auto it : journal_list){
            name = "/mnt/cephfs" + it;
            pos  = sizeof(journal_file_header_t);
            File* file = new File(name, pos, false); 
            cidx = m_router->route(file->fid, m_consumer.size());
            m_consumer[cidx]->enqueue(file);
        }

        if(limit == journal_list.size()){
            /*more journal file again, renew latest marker*/ 
            auto rit = journal_list.rbegin();
            m_latest_marker.set_cur_journal(*rit);
            m_latest_marker.set_pos(0);
        } else {
            /*notify next chain that here no file any more*/
            m_run = false;
            broadcast_consumer_exit();
            LOG_INFO << "src worker discharge out";
            break;
        }
    }    
}

void SrcWorker::enqueue(void* item)
{
    return;
}


void CacheRecovery::start()
{
    LOG_INFO << "CacheRecovery start";
    m_processor = new ProcessWorker[m_processor_num];
    for(int i = 0; i < m_processor_num; i++){
        /*todo: here use a trick*/
        new (&m_processor[i]) ProcessWorker(m_id_generator, m_cache_proxy);
        m_processor[i].start();
    }
    
    m_src_worker = new SrcWorker(m_volume, m_grpc_client);
    for(int i = 0; i < m_processor_num; i++){
        m_src_worker->register_consumer(&m_processor[i]);
        m_processor[i].register_producer(m_src_worker);
    }
    
    m_src_worker->start();
    
    LOG_INFO << "CacheRecovery start ok";
}

void CacheRecovery::stop()
{
    LOG_INFO << "CacheRecovery stop";
    if(m_src_worker){
        m_src_worker->stop();
        delete m_src_worker;
    }
    
    if(m_processor){
        for(int i = 0; i < m_processor_num; i++){
            m_processor[i].stop();
        }
        delete [] m_processor;
    }
    
    LOG_INFO << "CacheRecovery stop ok";
}
