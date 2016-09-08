#include <stdlib.h>
#include <string.h>
#include "cache_recover.h"

void ProcessWorker::start()
{
    m_router = new HashRoute();
    m_que    = new queue<IReadFile*>();
    m_cond   = new cond();
    m_mutex  = new mutex();

    if(m_consumer.size() > 0){
        m_run = true;
        m_thread = new thread(bind(&ProcessWorker::loop, this));
    }
}
   
void ProcessWorker::stop()
{
    if(m_thread){
        m_thread->join();
        delete m_thread;
    }
    if(m_mutex){
        delete m_mutex;
    }
    if(m_cond){
        delete m_cond;
    }
    if(m_que){
        delete m_que;
    }
    if(m_router){
        delete m_router;
    }
    m_consumer.clear();
}

void ProcessWorker::loop()
{
    unique_lock<std::mutex> ulock(*m_mutex);
    while(m_run){
        while(m_que && m_que->empty()){
            m_cond->wait(ulock);
            usleep(100);
        } 

        while(m_que && !m_que->empty()){
            IReadFile* file = m_que->front();
            cout << "Process file Item file:" << file->m_file << "eos:" 
                << file->m_eos << endl; 
            if(!file->m_eos){
                /*the last file, exit cache recover */
                delete file;
                m_run = false;
                break;
            } else {
                /*normal, read each file, and add to cache*/
                process_file(file); 
            }
            delete file; 
            m_que->pop();
        }
    }
}
   
void ProcessWorker::enqueue(void* item)
{
    unique_lock<std::mutex> ulock(*m_mutex);
    m_que->push((IReadFile*)item);
    m_cond->notify_one();
}

void ProcessWorker::process_file(IReadFile* file)
{
    /*prepare id generator, add file*/
    m_id_generator->add_file(file->m_file);

    int ret = file->open(); 
    if(ret != -1){
        cout << "read open failed ret=" << ret << endl;
        return;
    }

    off_t start = (file->m_pos == 0)?(file->m_pos+sizeof(journal_header_t))
                    : file->m_pos;
    off_t end = file->m_size;

    while(start < end){
        /*to do: optimize read*/
        string log_file = file->m_file;
        off_t  log_off  = start;
        shared_ptr<ReplayEntry> entry = nullptr; 
        size_t ret = file->read_entry(start, m_buffer_pool, entry);
        if(nullptr == entry || ret != entry->length()){
            cout << "[process worker] read entry failed " << endl; 
            break;
        }
        start += ret;
        
        /*todo:generate log sequence*/
        m_cache_proxy->write(log_file, log_off, entry);
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
}

void SrcWorker::loop()
{
    while(m_run){
        /*get journal file from drserver*/
        int limit = 10;
        list<string> journal_list; 
        int ret = m_grpc_client->GetJournalList(m_volume, *m_latest_marker, 
                                                limit, journal_list);
        if(!ret || journal_list.empty()){
            cout << "srcworker getjournallist failed"<<endl;
            m_run = true;
            break;
        }

        /*dispatch file to next chain*/
        auto it = journal_list.begin();
        for(; it != journal_list.end(); it++){
            string     name = *it;
            uint64_t   pos; 
            IReadFile* file = nullptr;
            if(*it == m_latest_marker->cur_journal()){
                pos = m_latest_marker->pos();
            } else {
                pos = 0;
            }
            file = new SyncReadFile(name, pos, false); 
            int cidx = m_router->route(file->fid, m_consumer.size());
            m_consumer[cidx]->enqueue(file);
        }

        if(limit != journal_list.size()){
            /*no more journal file, exist*/
            m_run = false;
            /*notify next chain that here no file any more*/
            IReadFile* file = new SyncReadFile("", -1, true);
            for(auto it : m_consumer){
                it->enqueue(file);
            }
            break;
        } else {
            /*more journal file again, renew latest marker*/ 
            auto rit = journal_list.rbegin();
            m_latest_marker->set_cur_journal(*rit);
            m_latest_marker->set_pos(0);
        }
    }    
}

void SrcWorker::enqueue(void* item)
{
    return;
}


void CacheRecovery::start()
{
    m_processor = new ProcessWorker[m_processor_num];
    for(int i = 0; i < m_processor_num; i++){
        /*todo: here use a trick*/
        new (&m_processor[i]) ProcessWorker(m_id_generator, m_cache_proxy);
        m_processor[i].init(m_buffer_pool);
        m_processor[i].start();
    }
    
    m_src_worker = new SrcWorker(m_volume, m_grpc_client, m_latest_marker);
    for(int i = 0; i < m_processor_num; i++){
        m_src_worker->register_consumer(&m_processor[i]);
        m_processor[i].register_producer(m_src_worker);
    }
    
    /*get latest journalmarker*/
    bool ret = m_grpc_client->GetJournalMarker(m_volume, *m_last_marker); 
    if(!ret){
        cout << " CacheRecovery getjournalmarker failed"<<endl;
        return;
    }
    m_latest_marker = m_last_marker;

    m_src_worker->start();

    cout << "CacheRecovery start ok" << endl;
}

void CacheRecovery::stop()
{
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
    
    ///*here all recover done, update journal marker*/
    //if(m_last_marker != m_latest_marker){
    //    bool ret = m_grpc_client->UpdateConsumerMarker(*m_latest_marker, m_volume);
    //    if(!ret){
    //        cout << " CacheRecovery updatejournalmarker failed"<<endl;
    //    }
    //}

    cout << "CacheRecovery stop" << endl;
}

