#include "journal_writer.hpp"
#include "../log/log.h"

#include <cerrno>

namespace Journal{

JournalWriter::JournalWriter(BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                             BlockingQueue<struct IOHookReply*>& reply_queue)
    :thread_ptr(),
     write_queue_(write_queue),
     reply_queue_(reply_queue),
     cur_file_ptr(NULL),
     cur_journal_size(0),
     cur_lease_journal("", "")
{
}

JournalWriter::~JournalWriter()
{
    if(NULL != cur_file_ptr)
    {
        fclose(cur_file_ptr);
        //cur_journal = NULL;
    }

    //todo seal journals
    while(!seal_queue.empty())
    { 
        //If we don't seal again, then we'll never have the chance to do that again.
        seal_journals(lease_client_->get_lease());
    }
}


bool JournalWriter::init(string vol,
                         string rpc_addr,
                         shared_ptr<ConfigParser> conf,
                         shared_ptr<IDGenerator> idproxy,
                         shared_ptr<CacheProxy> cacheproxy,
                         shared_ptr<SnapshotProxy> snapshotproxy,
                         shared_ptr<CephS3LeaseClient> lease_client)
 
{
    vol_id = vol;
    idproxy_ = idproxy;
    cacheproxy_ = cacheproxy;
    snapshot_proxy_ = snapshotproxy;
    running_flag = true;
    lease_client_ = lease_client;
    cur_journal_size = 0;

    rpc_client.reset(new WriterClient(grpc::CreateChannel(rpc_addr, 
                        grpc::InsecureChannelCredentials())));

    std::string mnt = "/mnt/cephfs";
    config.journal_max_size = conf->get_default<int>("journal_writer.journal_max_size",32 * 1024 * 1024);
    config.journal_mnt = conf->get_default("journal_writer.mnt",mnt);
    config.write_timeout = conf->get_default("journal_writer.write_timeout",2);
    config.version = conf->get_default("journal_writer.version",0);
    config.checksum_type = (checksum_type_t)conf->get_default("pre_processor.checksum_type",0);
    config.journal_limit = conf->get_default("ceph_s3.journal_limit",4);

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
    shared_ptr<JournalEntry> entry = nullptr;
    uint64_t entry_size = 0;
    uint64_t write_size = 0;

    while(true)
    {
        bool ret = write_queue_.pop(entry);
        if(!ret){
            return; 
        }

        if (running_flag == false)
            return;
        
        success = false;
        time(&start);
        time(&end);
        entry_size = entry->get_persit_size();

        while(!success && (difftime(end,start) < config.write_timeout))
        {
            if(!open_journal(entry_size))
            {
                time(&end);
                continue;
            }

            std::string journal_file = config.journal_mnt + cur_lease_journal.second;
            off_t journal_off = cur_journal_size;
            
            /*persist to journal file*/
            write_size = entry->persist(cur_file_ptr, journal_off);
            if(write_size != entry_size)
            {
                LOG_ERROR << "write journal file: " << cur_lease_journal.second
                          << " failed:" << strerror(errno);
                cur_journal_size += write_size;
                time(&end);
                continue;
            }
            /*clear message serialized data*/
            entry->clear_serialized_data();
            
            /*snapshot cmd synchronize as soon as possible*/
            if(entry->get_type() == SNAPSHOT_CREATE ||
               entry->get_type() == SNAPSHOT_DELETE ||
               entry->get_type() == SNAPSHOT_ROLLBACK){
                LOG_INFO << "journal write reply snapshot command";
                snapshot_proxy_->cmd_persist_notify(); 
            }

            /*add to cache*/
            cacheproxy_->write(journal_file, journal_off, entry);

            cur_journal_size += write_size;
            journal_off = cur_journal_size;

            success = true;
        }

        if(entry->get_type() == IO_WRITE){
            send_reply(entry.get(),success);
        } else {
            ;
        }
    }
}

//The caller should check_lease_validity first
bool JournalWriter::get_journal()
{
    cur_journal_size = 0;
    cur_lease_journal = std::make_pair("", "");

    {
        std::unique_lock<std::recursive_mutex> journal_uk(journal_mtx_);
        if(journal_queue.empty())
        {
            LOG_INFO << "journal_queue empty";
            get_writeable_journals(lease_client_->get_lease(),config.journal_limit);
        }

        if (journal_queue.empty())
        {
            return false;
        }

        cur_lease_journal = journal_queue.front();
        journal_queue.pop();
        LOG_INFO << "journal_queue pop journal";
    }

    if (cur_lease_journal.first != "")
    {
        if (!lease_client_->check_lease_validity(cur_lease_journal.first))
        {
            // check lease valid failed
            handle_lease_invalid();
            LOG_ERROR << "check lease validity result:false";
            return false;
        }
    }
    else
    {
        // lease is null
        handle_lease_invalid();
        return false;
    }

    if(cur_lease_journal.second == "")
    {
        return false;
    }
    return true;
}

bool JournalWriter::open_journal(uint64_t entry_size)
{
    if (cur_lease_journal.second == "")
    {
        if(!get_journal())
            return false;
    }

    // check lease valid for this journal
    if (cur_lease_journal.first != ""){
        if (!lease_client_->check_lease_validity(cur_lease_journal.first))
        {
            // check lease vaildity failed
            handle_lease_invalid();
            LOG_ERROR << "check lease validity result:false";

            if(!get_journal())
                return false;
        }
    }
    else
    {
        handle_lease_invalid();
        return false;
    }

    if((entry_size + cur_journal_size) > config.journal_max_size)
    {
        seal_queue.push(cur_lease_journal);
        LOG_INFO << "push journal:" << cur_lease_journal.second << "to seal queue ok";
        if(NULL != cur_file_ptr)
        {
            fclose(cur_file_ptr);
            cur_file_ptr = NULL;
        }

        if(!get_journal())
            return false;
    }

    if (NULL == cur_file_ptr)
    {
        std::string tmp = config.journal_mnt + cur_lease_journal.second;
        cur_file_ptr = fopen(tmp.c_str(), "ab+");
        if(NULL == cur_file_ptr)
        {
             LOG_ERROR << "open journal file: " << cur_lease_journal.second
                       << " failed:" << strerror(errno);
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
    journal_file_header_t journal_header;
    journal_header.version = config.version;

    if(fwrite(&journal_header,sizeof(journal_file_header_t),1,cur_file_ptr) != 1)
    {
        LOG_ERROR << "write journal header faied,journal:" << cur_lease_journal.second
                  << "errno:" << strerror(errno);
        return false;
    }
    cur_journal_size += sizeof(journal_file_header_t);
    return true;
}

bool JournalWriter::get_writeable_journals(const std::string& uuid,const int32_t limit)
{
    std::list<std::string> journals;
    int32_t tmp = 0;
    
    std::unique_lock<std::mutex> lk(rpc_mtx_);
    std::unique_lock<std::recursive_mutex> journal_uk(journal_mtx_);

    if(journal_queue.size() >= limit)
    {
       return true;
    }
    else
    {
        tmp = limit - journal_queue.size();
    }
    if(!rpc_client->GetWriteableJournals(uuid,vol_id,tmp,journals))
    {
        LOG_ERROR << "get journal file failed";
        return false;
    }

    for(auto tmp:journals)
    {
        journal_queue.push(std::make_pair(uuid, tmp));
    }
    return true;
}

bool JournalWriter::seal_journals(const std::string& uuid)
{
    std::pair<std::string, std::string> tmp;
    std::list<std::string> journals;
    std::list<std::pair<std::string, std::string>> backup;

    std::unique_lock<std::mutex> lk(rpc_mtx_);
    std::unique_lock<std::mutex> seal_uk(seal_mtx_);

    while(!seal_queue.empty())
    {
        tmp = seal_queue.front();
        // only seal these journals with valid lease
        if (tmp.second != "" && uuid == tmp.first)
        {
            journals.push_back(tmp.second);
            backup.push_back(tmp);
        }
        seal_queue.pop();
    }

    if (!journals.empty())
    {
        if(!rpc_client->SealJournals(uuid, vol_id, journals))
        {
            LOG_ERROR << "SealJournals failed";
            for (auto k: backup)
            {
                seal_queue.push(k);
            }
            return false;
        }
        else
        {
            LOG_INFO << "SealJournals ok";
            return true;
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

void JournalWriter::send_reply(JournalEntry* entry,bool success)
{
    vector<uint64_t> handles = entry->get_handle();
    for(uint64_t handle : handles){
        IOHookReply* reply_ptr = (IOHookReply*)new char[sizeof(IOHookReply)];
        reply_ptr->magic = MESSAGE_MAGIC;
        reply_ptr->error = success?0:1;
        reply_ptr->handle = handle;
        reply_ptr->len = 0;
        if(!reply_queue_.push(reply_ptr)){
            LOG_ERROR << "reply queue push failed";
            delete []reply_ptr;
            return;
        }
    }
}

void JournalWriter::handle_lease_invalid()
{
    cur_lease_journal = std::make_pair("", "");
    cur_journal_size = 0;
}
}


