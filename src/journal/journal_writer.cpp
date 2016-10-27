#include "journal_writer.hpp"
#include "../log/log.h"

#include <cerrno>

namespace Journal{

JournalWriter::JournalWriter(std::string rpc_addr,
                                   entry_queue& write_queue,std::condition_variable& cv,
                                   reply_queue& rep_queue,std::condition_variable& reply_cv)
    :rpc_client(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials())),
    thread_ptr(),
    write_queue_(write_queue),
    reply_queue_(rep_queue),
    cur_file_ptr(NULL),
    cur_journal(NULL),
    cur_journal_size(0),
    journal_queue_size(0),
    cv_(cv),
    reply_cv_(reply_cv)
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


bool JournalWriter::init(std::string& vol,
                         ConfigParser& conf,
                         shared_ptr<IDGenerator> idproxy,
                         shared_ptr<CacheProxy> cacheproxy)
 
{
    vol_id = vol;
    idproxy_ = idproxy;
    cacheproxy_ = cacheproxy;
    running_flag = true;
    cur_journal_size = 0;
    write_seq = 0;

    std::string mnt = "/mnt/cephfs";
    config.journal_max_size = conf.get_default<int>("journal_writer.journal_max_size",32 * 1024 * 1024);
    config.journal_mnt = conf.get_default("journal_writer.mnt",mnt);
    config.write_timeout = conf.get_default("journal_writer.write_timeout",2);
    config.version = conf.get_default("journal_writer.version",0);
    config.checksum_type = (checksum_type_t)conf.get_default("pre_processor.checksum_type",0);
    config.journal_limit = conf.get_default("ceph_s3.journal_limit",4);
    thread_ptr.reset(new boost::thread(boost::bind(&JournalWriter::work, this)));
    return true;
}

bool JournalWriter::deinit()
{
    running_flag = false;
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
        while(running_flag && !(entry = get_entry()))
        {
            cv_.wait_for(lk,std::chrono::seconds(2));
        }
        if (running_flag == false)
            return;
        success = false;
        write_seq++;
        time(&start);
        time(&end);
        entry_size = entry->length();
        while(!success && (difftime(end,start) < config.write_timeout))
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
            
            std::string log_file = config.journal_mnt + *cur_journal;
            off_t log_off = cur_journal_size;
            shared_ptr<ReplayEntry> log_entry(entry);
            cacheproxy_->write(log_file, log_off, log_entry);

            cur_journal_size = cur_journal_size + write_size;
            success = true;
        }
        entry_map.erase(write_seq);
        lk.unlock();
        send_reply(entry,success);
    }
}
bool JournalWriter::get_journal()
{
    if(journal_queue.empty())
    {
        get_writeable_journals("test-uuid",config.journal_limit);
    }
    if(!journal_queue.pop(cur_journal))
    {
        LOG_ERROR << "journal_queue pop failed";
        return false;
    }
    journal_queue_size--;
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

    if((entry_size + cur_journal_size) > config.journal_max_size)
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
        std::string tmp = config.journal_mnt + *cur_journal;
        cur_file_ptr = fopen(tmp.c_str(), "ab+");
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

        idproxy_->add_file(tmp);
    }
    return true;
}

bool JournalWriter::write_journal_header()
{
    journal_header_t journal_header;
    journal_header.version = config.version;
    journal_header.checksum_type = config.checksum_type;
    if(fwrite(&journal_header,sizeof(journal_header),1,cur_file_ptr) != 1)
    {
        LOG_ERROR << "write journal header faied,journal:" << cur_journal << "errno:" << strerror(errno); 
        return false;
    }
    cur_journal_size += sizeof(journal_header);
    return true;
}

bool JournalWriter::get_writeable_journals(const std::string& uuid,const int32_t limit)
{
    std::unique_lock<std::mutex> lk(rpc_mtx_);
    std::list<std::string> journals;
    int32_t tmp = 0;
    if(journal_queue_size >= limit)
    {
        return true;
    }
    else
    {
        tmp = limit - journal_queue_size;
    }
    if(!rpc_client.GetWriteableJournals(uuid,vol_id,tmp,journals))
    {
        LOG_ERROR << "get journal file failed";
        return false;
    }
    for(auto tmp:journals)
    {
        std::string * journal_ptr = new std::string(tmp);
        journal_queue.push(journal_ptr);
        journal_queue_size++;
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

void JournalWriter::send_reply(ReplayEntry* entry,bool success)
{
    IOHookReply* reply_ptr = (IOHookReply*)new char[sizeof(IOHookReply)];
    reply_ptr->magic = MESSAGE_MAGIC;
    reply_ptr->error = success?0:1;
    reply_ptr->handle = entry->req_id();
    reply_ptr->len = 0;
    if(!reply_queue_.push(reply_ptr))
    {
        LOG_ERROR << "reply queue push failed";
        delete []reply_ptr;
        return;
    }
    reply_cv_.notify_one();
}

ReplayEntry* JournalWriter::get_entry()
{
    update_entry_map();
    EntryMap::iterator it = entry_map.find(write_seq);
    if(it != entry_map.end())
    {
        return it->second;
    }
    return NULL;
}

void JournalWriter::update_entry_map()
{
    ReplayEntry* entry = NULL;
    while(!write_queue_.empty())
    {
        if(write_queue_.pop(entry))
        {
            entry_map.insert(std::pair<uint64_t,ReplayEntry*>(entry->get_req_seq(),entry));
        }
    }
}

}
