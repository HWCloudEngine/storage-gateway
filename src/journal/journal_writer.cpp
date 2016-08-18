#include <boost/asio.hpp>
#include "journal_writer.hpp"
#include "../log/log.h"
#include <cstdio>
#include <cerrno>

namespace Journal{

JournalWriter::JournalWriter(std::string rpc_addr,
                                   entry_queue& write_queue,
                                   boost::asio::ip::tcp::socket& raw_socket,
                                   std::mutex& mtx,
                                   std::condition_variable& cv)
    :rpc_client(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials())),
    thread_ptr(),
    raw_socket_(raw_socket),
    write_queue_(write_queue),
    file_ptr(NULL),
    mtx_(mtx),
    cv_(cv)
{
}

JournalWriter::~JournalWriter()
{
    if(NULL != file_ptr)
    {
        fclose(file_ptr);
        file_ptr = NULL;
    }
}


bool JournalWriter::init(std::string& vol)
{
    vol_id = vol;
    //todo read from config.ini
    journal_max_size = 64 * 1024 * 1024;
    write_seq = 0;
    timeout = 2;
    thread_ptr.reset(new boost::thread(boost::bind(&JournalWriter::work, this)));
    thread_ptr->join();
    return true;
}

bool JournalWriter::deinit()
{
    //todo
    return true;
}

void JournalWriter::work()
{
    bool success;
    time_t start,end;
    ReplayEntry* entry;
    uint64_t entry_size;
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
        success = false;
        time(&start);
        time(&end);
        entry_size = entry->length();
        while(!success && (difftime(end,start) < timeout))
        {
            if(!get_journal(entry_size))
            {
                time(&end);
                continue;
            }
            if(fwrite(entry->data(),1,entry_size,file_ptr) != entry_size)
            {
                //todo handle the error
                LOG_ERROR << "write journal file: " << journals.front() << " failed:" << strerror(errno);
                time(&end);
                continue;
            }
            fflush(file_ptr);
            //todo cache entry
            success = true;
            write_seq++;
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

bool JournalWriter::get_journal(uint64_t entry_size)
{
    std::string journal;
    if(journals.empty())
    {
        //todo get uuid
        if(!get_writeable_journals("test-uuid",1))
        {
            return false;
        }
    }
    
    journal = journals.front();
    int64_t journal_size = get_file_size(journal.c_str());
    if(journal_size < 0)
    {
        LOG_ERROR << "get journal file size failed" << strerror(errno);
        return false;
    }
    
    if((entry_size + journal_size) > journal_max_size)
    {
        std::list<std::string> tmp;
        tmp.push_back(journal);
        if(!seal_journals("test-uuid",tmp))
        {
            LOG_ERROR << "seal journal: " << journal << " failed";
            return false;
        }
        journals.pop_front();
        if(NULL != file_ptr)
        {
            fclose(file_ptr);
            file_ptr = NULL;
        }
        if(journals.empty())
        {
            if(!get_writeable_journals("test-uuid",1))
            {
                return false;
            }
        }
        journal = journals.front();
    }
    
    if (NULL ==file_ptr)
    {
        file_ptr = fopen(journal.c_str(), "ab+");
        if(NULL == file_ptr)
        {
             LOG_ERROR << "open journal file: " << journal << " failed:" << strerror(errno);
             return false;
        }
    }
    return true;
}

bool JournalWriter::get_writeable_journals(const std::string& uuid,const int limit)
{
    if(!rpc_client.GetWriteableJournals(uuid,vol_id,limit,journals))
    {
        LOG_ERROR << "get journal file failed";
        return false;
    }
    return true;
}

bool JournalWriter::seal_journals(const std::string& uuid, const std::list<std::string>& list_)
{
    if(!rpc_client.SealJournals(uuid,vol_id,list_))
    {
        return false;
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

