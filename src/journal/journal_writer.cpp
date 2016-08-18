#include <boost/asio.hpp>
#include "journal_writer.hpp"
#include <cerrno>

namespace Journal{

JournalWriter::JournalWriter(std::string rpc_addr,
                                   entry_queue& write_queue,
                                   boost::asio::ip::tcp::socket& raw_socket,
                                   std::condition_variable& cv)
    :rpc_client(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials())),
    thread_ptr(),
    raw_socket_(raw_socket),
    write_queue_(write_queue),
    cur_file_ptr(NULL),
    cur_journal(NULL),
    cur_journal_size(0),
    cv_(cv)
{
}

JournalWriter::~JournalWriter()
{
    if(NULL != cur_file_ptr)
    {
        fclose(cur_file_ptr);
        cur_journal = NULL;
    }
    if(NULL != cur_journal)
    {
        delete cur_journal;
        cur_journal = NULL;
    }
    //todo delete the journal and seal
    std::string * tmp = NULL;
    while(!journal_queue.empty())
    {
        if(journal_queue.pop(tmp) && (NULL!= tmp))
            delete tmp;
    }
    while(!seal_queue.empty())
    { 
        //If we don't seal again, then we'll never have the chance to do that again.
        seal_journals("test-uuid");
    }
}


bool JournalWriter::init(std::string& vol)
{
    vol_id = vol;
    //todo read from config.ini
    cur_journal_size = 0;
    journal_max_size = 32 * 1024 * 1024;
    write_seq = 0;
    write_timeout = 2;
    version = 0;
    checksum_type = CRC_32;
    thread_ptr.reset(new boost::thread(boost::bind(&JournalWriter::work, this)));
    return true;
}

bool JournalWriter::deinit()
{
    thread_ptr->interrupt();
    thread_ptr->join();

    return true;
}

void JournalWriter::work()
{
    bool success = false;
    time_t start,end;
    ReplayEntry* entry = NULL;
    uint64_t entry_size = 0;
    uint64_t write_size = 0;
    while(true)
    {
        std::unique_lock<std::mutex> lk(mtx_);
        while(write_queue_.empty())
        {
            cv_.wait(lk);
        }
        if(!write_queue_.pop(entry))
        {
            LOG_ERROR << "write_queue pop failed";
            continue;
        }
        if (NULL == entry)
        {
            LOG_ERROR << "entry ptr NULL";
            continue;
        }
        success = false;
        time(&start);
        time(&end);
        entry_size = entry->length();
        while(!success && (difftime(end,start) < write_timeout))
        {
            if(!open_journal(entry_size))
            {
                time(&end);
                continue;
            }
            //todo to be enhanced to aio
            write_size = fwrite(entry->data(),1,entry_size,cur_file_ptr);
            if(write_size != entry_size)
            {
                LOG_ERROR << "write journal file: " << cur_journal<< " failed:" << strerror(errno);
                cur_journal_size = cur_journal_size + write_size;
                time(&end);
                continue;
            }
            fflush(cur_file_ptr);
            cur_journal_size = cur_journal_size + write_size;
            success = true;
            write_seq++;
            delete entry;
            entry = NULL;
            //todo cache entry
        }
        lk.unlock();
        if(success)
        {
            //todo send msg to IOHook
            ;
        }
        else
        {
            //todo send error msg to IOHook
            ;
        }
    }
}
bool JournalWriter::get_journal()
{
    if(journal_queue.empty())
    {
        std::list<std::string> journals;
        if(!rpc_client.GetWriteableJournals("test-uuid",vol_id,1,journals))
        {
            LOG_ERROR << "get journal file failed";
            return false;
        }
        for(auto tmp:journals)
        {
            std::string * journal_ptr = new std::string(tmp);
            journal_queue.push(journal_ptr);
        }
    }
    if(!journal_queue.pop(cur_journal))
    {
        LOG_ERROR << "journal_queue pop failed";
        return false;
    }
    if(cur_journal == NULL)
        return false;
    return true;
}

bool JournalWriter::open_journal(uint64_t entry_size)
{
    if (cur_journal == NULL)
    {
        if(!get_journal())
            return false;
    }

    if((entry_size + cur_journal_size) > journal_max_size)
    {
        if(!seal_queue.push(cur_journal))
        {
            LOG_ERROR << "push journal:" << *cur_journal << "to seal queue failed";
            return false;
        }
        if(NULL != cur_file_ptr)
        {
            fclose(cur_file_ptr);
            cur_file_ptr = NULL;
        }
        cur_journal = NULL;
        cur_journal_size = 0;
        if(!get_journal())
            return false;
    }
    
    if (NULL == cur_file_ptr)
    {
        cur_file_ptr = fopen(cur_journal->c_str(), "ab+");
        if(NULL == cur_file_ptr)
        {
             LOG_ERROR << "open journal file: " << *cur_journal << " failed:" << strerror(errno);
             return false;
        }
        else
        {
            if(!write_journal_header())
                return false;
        }
    }
    return true;
}

bool JournalWriter::write_journal_header()
{
    journal_header_t journal_header;
    journal_header.version = version;
    journal_header.checksum_type = checksum_type;
    if(fwrite(&journal_header,sizeof(journal_header),1,cur_file_ptr) != 1)
    {
        LOG_ERROR << "write journal header faied,journal:" << cur_journal << "errno:" << strerror(errno); 
        return false;
    }

}

bool JournalWriter::get_writeable_journals(const std::string& uuid,const int limit)
{
    std::list<std::string> journals;
    if(!rpc_client.GetWriteableJournals(uuid,vol_id,limit,journals))
    {
        LOG_ERROR << "get journal file failed";
        return false;
    }
    for(auto tmp:journals)
    {
        std::string * journal_ptr = new std::string(tmp);
        journal_queue.push(journal_ptr);
    }
    return true;
}

bool JournalWriter::seal_journals(const std::string& uuid)
{
    std::list<std::string*> backup;
    std::list<std::string> journals;
    std::string * tmp = NULL;
    while(!seal_queue.empty())
    {
        if(seal_queue.pop(tmp))
        {
            backup.push_back(tmp);
            journals.push_back(*tmp);
        }
    }
    if(!journals.empty())
    {
        if(!rpc_client.SealJournals(uuid,vol_id,journals))
        {
            for(auto k:backup)
            {
                seal_queue.push(k);
            }
            return false;
        }
        else
        {
            for(auto i:backup)
            {
                delete i;
            }
        }
    }
    return true;    
}

int64_t JournalWriter::get_file_size(const char *path) 
{
    int64_t filesize = -1;
    struct stat statbuff;
    if(stat(path, &statbuff) < 0)
    {
        return filesize;  
    }
    else
    {
        filesize = statbuff.st_size;  
    }
    return filesize;
}
}

