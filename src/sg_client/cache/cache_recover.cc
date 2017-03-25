#include <stdlib.h>
#include <string.h>
#include "cache_recover.h"
#include "../../common/journal_entry.h"

void IWorker::register_consumer(IWorker* worker)
{
    m_consumer.push_back(worker);
}

void IWorker::register_producer(IWorker* worker)
{
    m_producer.push_back(worker);
}

ProcessWorker::ProcessWorker(shared_ptr<IDGenerator> id_maker, 
                             shared_ptr<CacheProxy> cache_proxy)
           :m_id_generator(id_maker), m_cache_proxy(cache_proxy)
{
}
 
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
            LOG_INFO << "process worker doing";
        } else {
            /*no more file , process exit*/
            m_run = false;
            LOG_INFO << "process worker done";
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
        LOG_ERROR << "open file:" << file->m_file << "failed";
        return;
    }

    LOG_INFO << "Process file:" << file->m_file << " start_pos:"  << file->m_start_pos
             << " end_pos:" << file->m_end_pos << " size:" << file->m_size 
             << " eos:"  << file->m_eos; 

    assert(file->m_size != 0);

    off_t start = file->m_start_pos;
    off_t end   = file->m_end_pos;
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

    file->close();
    m_id_generator->del_file(file->m_file);
}

SrcWorker::SrcWorker(Configure& conf, string vol, 
                     shared_ptr<ReplayerClient> rpc_cli)
        :m_conf(conf), m_volume(vol), m_grpc_client(rpc_cli)
{
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
    bool ret = m_grpc_client->GetJournalMarker(m_volume, m_latest_marker); 
    if(!ret){
        LOG_ERROR << "src worker get journal marker failed";
        broadcast_consumer_exit();
        return;
    }

    while(m_run){
        /*get journal file list from drserver*/
        const int limit = 10;
        list<JournalElement> journal_list; 
        ret = m_grpc_client->GetJournalList(m_volume, m_latest_marker, limit, 
                                            journal_list);
        if(!ret || journal_list.empty()){
            LOG_ERROR << "src worker get journal list failed";
            m_run = false;
            broadcast_consumer_exit();
            break;
        }
       
        /*dispatch file to process work*/
        for(auto it : journal_list){
            string name = m_conf.journal_mount_point + it.journal();
            off_t start_pos = it.start_offset() == 0 ? sizeof(journal_file_header_t) : it.start_offset();
            off_t end_pos = it.end_offset();
            File* file = new File(name, start_pos, false); 
            int cidx = m_router->route(file->fid, m_consumer.size());
            m_consumer[cidx]->enqueue(file);
        }

        if(limit == journal_list.size()){
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

void SrcWorker::enqueue(void* item)
{
    return;
}


CacheRecovery::CacheRecovery(Configure& conf, string volume, 
                             shared_ptr<ReplayerClient> rpc_cli, 
                             shared_ptr<IDGenerator> id_maker,
                             shared_ptr<CacheProxy> cache_proxy)
    :m_conf(conf),m_volume(volume),m_grpc_client(rpc_cli),m_id_generator(id_maker),
    m_cache_proxy(cache_proxy)
{

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
    
    m_src_worker = new SrcWorker(m_conf, m_volume, m_grpc_client);
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
