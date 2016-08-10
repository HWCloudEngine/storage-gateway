#include <stdlib.h>
#include <string.h>
#include "cache_recover.h"

ostream& operator<<(ostream& out, const ReplayElement& re)
{
    out << "[seq:" << re.m_log_seq << " file:" << re.m_log_file << " eos:" 
        << re.m_eos << "]";
    return out;
}

void DestWorker::start()
{
    m_que    = new queue<ReplayElement*>();
    m_cond   = new cond();
    m_mutex  = new mutex();
    m_run    = true;
    m_thread = new thread(bind(&DestWorker::loop, this));
}

void DestWorker::stop()
{
    m_thread->join();
    delete m_thread;
    delete m_mutex;
    delete m_cond;
    delete m_que;
}

void DestWorker::loop()
{
    unique_lock<std::mutex> ulock(*m_mutex);
    while(m_run){
        while(m_que && m_que->empty()){
            m_cond->wait(ulock);
            usleep(100);
        } 
        while(m_que && !m_que->empty()){
            ReplayElement* re = m_que->front();
            cout << "Recover:" << std::this_thread::get_id() << *re << endl;
            if(!re->m_eos){
                /*no any more replayelement, here exit*/
                m_run = false; 
                delete re;
                break;
            } else {
                /*replayelement, add cache*/
                /*todo: add to cache proxy*/
                delete re;
            }
            m_que->pop();
        }
    }
}
    
void DestWorker::inqueue(void* item)
{
    unique_lock<std::mutex> ulock(*m_mutex);
    m_que->push((ReplayElement*)item);
    m_cond->notify_one();
}


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
                /*the last file, notify next chain no any more replayelement*/
                ReplayElement* re = new ReplayElement(IoVersion(0, 0), "test", 
                                                      0, nullptr, true);
                for(auto it : m_consumer){
                    it->inqueue(re); 
                }
                delete file;
                m_run = false;
                break;
            } else {
                /*normal, read each file, dispatch replayelement to next chain*/
                process_file(file); 
            }
            delete file; 
            m_que->pop();
        }
    }
}
   
void ProcessWorker::inqueue(void* item)
{
    unique_lock<std::mutex> ulock(*m_mutex);
    m_que->push((IReadFile*)item);
    m_cond->notify_one();
}

size_t ProcessWorker::cal_data_size(off_len_t* off_len, int count)
{
    size_t data_size = 0;
    for(int i = 0 ; i < count; i++){
        data_size += off_len[i].length;
    }
    return data_size;
}
    
void ProcessWorker::process_file(IReadFile* file)
{
    /*prepare id generator, add file*/
    m_id_generator->add_file(file->m_file);

    int ret = file->open(); 
    if(!ret){
        cout << "read open failed ret=" << ret << endl;
        return;
    }

    off_t start   = file->m_pos;
    off_t end     = file->m_pos + file->m_size;
    off_t log_off = 0;
    while(start < end){
        /*to do: optimize read*/
        
        /*record log off*/
        log_off = start;

        /*read log head*/
        log_header_t log_head;
        memset(&log_head, 0, sizeof(log_head));
        size_t head_size = sizeof(log_head);
        int ret = file->read(start, (char*)&log_head, head_size);
        if(ret != sizeof(log_head)){
            cout << "read log head failed ret=" << ret << endl;
            break;
        }
        start += head_size;

        /*read off len */
        size_t off_len_size = log_head.count * sizeof(off_len_t);
        off_len_t* off_len = (off_len_t*)malloc(off_len_size);
        ret = file->read(start, (char*)off_len, off_len_size);
        if(ret != off_len_size){
            cout << "read log off len failed ret=" << ret << endl;
            break;
        }
        start += off_len_size;

        /*read data*/
        size_t data_size = cal_data_size(off_len, log_head.count);
        size_t entry_data_len = head_size + off_len_size + data_size;
        char*  entry_data = (char*)malloc(entry_data_len);
        memcpy(entry_data, &log_head, head_size);
        memcpy(entry_data+head_size, off_len, off_len_size);
        ret = file->read(start, entry_data+head_size+off_len_size, data_size);
        if(ret != data_size){
            cout << "read log data failed ret=" << ret << endl;
            break;
        }
        free(off_len);

        /*todo:generate log sequence*/
        IoVersion iov = m_id_generator->get_version(file->m_file);

        /*todo:generate replayelement*/
        shared_ptr<ReplayEntry> entry = nullptr; 

        ReplayElement* fre = new ReplayElement(iov, 
                                               file->m_file, 
                                               log_off, 
                                               entry, false);

        /*dispatch replayelement to next chain*/
        int cidx = m_router->route(fre->hashcode(), m_consumer.size());
        cout << "Process:" << std::this_thread::get_id() 
             << " log_seq:"<<iov << " cidx:" << cidx << endl;
        m_consumer[cidx]->inqueue(fre);
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
            m_consumer[cidx]->inqueue(file);
        }

        if(limit != journal_list.size()){
            /*no more journal file, exist*/
            m_run = false;
            /*notify next chain that here no file any more*/
            IReadFile* file = new SyncReadFile("", -1, true);
            for(auto it : m_consumer){
                it->inqueue(file);
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

void SrcWorker::inqueue(void* item)
{
    return;
}


void CacheRecovery::start()
{
    m_dest_worker = new DestWorker();
    m_dest_worker->start();

    m_processor = new ProcessWorker[m_processor_num]();
    for(int i = 0; i < m_processor_num; i++){
        m_processor[i].register_consumer(m_dest_worker);
        m_dest_worker->register_producer(&m_processor[i]);
        m_processor[i].start();
    }
    
    m_src_worker = new SrcWorker();
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
    
    if(m_dest_worker){
        m_dest_worker->stop();
        delete m_dest_worker;
    }
   
    /*here all recover done, update journal marker*/
    if(m_last_marker != m_latest_marker){
        bool ret = m_grpc_client->UpdateConsumerMarker(*m_latest_marker, m_volume);
        if(!ret){
            cout << " CacheRecovery updatejournalmarker failed"<<endl;
        }
    }

    cout << "CacheRecovery stop" << endl;
}

