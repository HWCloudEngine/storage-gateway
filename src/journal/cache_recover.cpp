#include <stdlib.h>
#include <string.h>

#include "cache_recover.hpp"

IReadFile::IReadFile(string file, off_t pos, bool eos)
    :m_file(file), m_pos(pos),m_eos(eos)
{
}

int IReadFile::open()
{
    m_fd = ::open(m_file.c_str(), O_RDONLY);
    if(-1 == m_fd){
        cout << "open " << m_file.c_str() << "failed errno:" << errno << endl;
        return -1;
    }
    struct stat buf;
    int ret = fstat(m_fd, &buf);
    if(-1 == ret){
        cout << "stat " << m_file.c_str() << "failed errno:" << errno << endl;
        return -1;
    }
    m_size = buf.st_size;
}

void IReadFile::close()
{
    if(-1 != m_fd){
        ::close(m_fd);
    } 
}


int SyncReadFile::fid = 0;

SyncReadFile::SyncReadFile(string file, off_t pos, bool eos)
    :IReadFile(file, pos, eos)
{
    fid++;
}

SyncReadFile::~SyncReadFile()
{
}

size_t SyncReadFile::read(off_t off, char* buf, size_t count)
{
    if(off < m_pos || (off+count) > m_size){
        cout << "read para error off: " << off << " pos:" << m_pos << " size:" << m_size << endl;
        return 0;
    }

    size_t left = count;
    size_t read = 0;
    while(left > 0){
        int ret = pread(m_fd, buf+read, left, off+read);
        if(ret == -1 | ret == 0){
            cout << "pread left:" << left << "failed errno:" << errno << endl; 
            return ret;
        } 

        left -= ret;
        read += ret;
    }

    m_pos += count;
    return 0;
}
    
int SyncReadFile::hashcode()
{
    //todo: how to geneate hashcode of each file
    return fid;
}


ostream& operator<<(ostream& out, const ReplayElement& re)
{
    out << "[seq:" << re.m_log_seq << " file:" << re.m_log_file << " eos:" << re.m_eos << "]";
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
            //todo: add item to cache
            //todo: how to finish stream
            ReplayElement* re = m_que->front();
            cout << "Recover:" << std::this_thread::get_id() << *re << endl;
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
    m_thread->join();
    delete m_thread;
    delete m_mutex;
    delete m_cond;
    delete m_que;
    delete m_router;
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
            cout << "Process file Item file:" << file->m_file << "eos:" << file->m_eos << endl; 
            //todo: read item from file, dispatch to next module
            //todo: how to finish the stream
            if(file->m_eos){
                ReplayElement* re = new ReplayElement(IoVersion(0, 0), "test", 0, true);
                for(auto it : m_consumer){
                    it->inqueue(re); 
                }
                m_run = false;
                break;
            }

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
    int ret = file->open(); 
    if(!ret){
        cout << "read open failed ret=" << ret << endl;
        return;
    }

    off_t start = file->m_pos;
    off_t end = file->m_pos + file->m_size;
    while(start < end){
        /*to do: optimize read*/

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

        /*generate log sequence*/
        //ReplayElement* fre = new ReplayElement(log_seq, file->m_file, 0);
        //int cidx = m_router->route(file->hashcode(), m_consumer.size());
        //cout << "Process:" << std::this_thread::get_id() << " log_seq:"<<log_seq << " cidx:" << cidx << endl;
        //m_consumer[cidx]->inqueue(fre);
    }
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
    m_thread->join();
    delete m_thread;
    m_consumer.clear();
    delete m_router;
}

void SrcWorker::loop()
{
    while(m_run){
        //todo: get journal file from drserver
        //todo: how to finish stream
        static int fileid = 0;
        if(fileid < 100){
            IReadFile* file = new SyncReadFile(to_string(fileid++), 0, false);    
            int cidx = m_router->route(file->hashcode(), m_consumer.size());
            cout <<"Fetcher:" << std::this_thread::get_id() 
                << "file:" << file->m_file << " cidx:" << cidx << endl;
            m_consumer[cidx]->inqueue(file);
            usleep(100);
        } else {
            IReadFile* file = new SyncReadFile(to_string(fileid++), 0, true);    
            cout <<"Fetcher:" << std::this_thread::get_id() 
                << "file:" << file->m_file << "all" << endl;
            for(auto it : m_consumer){
                it->inqueue(file); 
            }
            break;
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

    m_src_worker->start();

    cout << "CacheRecovery start" << endl;
}

void CacheRecovery::stop()
{
    m_src_worker->stop();

    for(int i = 0; i < m_processor_num; i++){
        m_processor[i].stop();
    }

    m_dest_worker->stop();
    
    delete m_src_worker;
    delete [] m_processor;
    delete m_dest_worker;

    cout << "CacheRecovery stop" << endl;
}


int main(int argc, char** argv)
{
    CacheRecovery* cr = new CacheRecovery();
    cr->start();
    cr->stop();
    pause();
    return 0;
}


