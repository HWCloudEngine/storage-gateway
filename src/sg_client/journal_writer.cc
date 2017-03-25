#include "journal_writer.h"
#include "rpc/message.pb.h"
#include "log/log.h"
#include <cerrno>

using huawei::proto::SnapshotMessage;

namespace Journal{

JournalWriter::JournalWriter(BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                             BlockingQueue<struct IOHookReply*>& reply_queue,
                             VolumeAttr& vol_attr)
    :thread_ptr(),
     write_queue_(write_queue),
     reply_queue_(reply_queue),
     vol_attr_(vol_attr),
     cur_file_ptr(NULL),
     cur_journal_size(0),
     written_size_since_last_update(0LLU),
     producer_marker_hold_flag(false)
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

    producer_event.unregister_from_epoll_fd(epoll_fd);
}


bool JournalWriter::init(const Configure& conf,
                         shared_ptr<IDGenerator> idproxy,
                         shared_ptr<CacheProxy> cacheproxy,
                         shared_ptr<SnapshotProxy> snapshotproxy,
                         shared_ptr<CephS3LeaseClient> lease_client,
                         shared_ptr<WriterClient> writer_client,
                         int _epoll_fd)
 
{
    conf_ = conf;

    idproxy_ = idproxy;
    cacheproxy_ = cacheproxy;
    snapshot_proxy_ = snapshotproxy;
    running_flag = true;
    lease_client_ = lease_client;
    cur_journal_size = 0;

    rpc_client = writer_client;
    epoll_fd = _epoll_fd;

    JournalElement e;
    e.set_journal("");
    cur_lease_journal = std::make_pair("", e);
    
    int e_fd = eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC);
    SG_ASSERT(e_fd != -1);
    producer_event.set_event_fd(e_fd);
    //Edge Triggered & wait for write event
    producer_event.set_epoll_events_type(EPOLLIN|EPOLLET);
    producer_event.set_epoll_data_ptr((void*)this);
    // register event
    SG_ASSERT(0 == producer_event.register_to_epoll_fd(epoll_fd));

    rpc_client.reset(new WriterClient(grpc::CreateChannel(conf_.sg_server_addr(), 
                        grpc::InsecureChannelCredentials())));

    thread_ptr.reset(new boost::thread(boost::bind(&JournalWriter::work, this)));
    return true;
}

bool JournalWriter::deinit()
{
    running_flag = false;
    thread_ptr->join();
    return true;
}

void JournalWriter::clear_producer_event(){
    producer_event.clear_event();
}

void JournalWriter::hold_producer_marker(){
    LOG_DEBUG << "hold producer marker,vol=" << vol_attr_.vol_name();
    producer_marker_hold_flag.store(true);
}

void JournalWriter::unhold_producer_marker(){
    LOG_DEBUG << "unhold producer marker,vol=" << vol_attr_.vol_name();
    producer_marker_hold_flag.store(false);
}

bool JournalWriter::is_producer_marker_holding(){
    return producer_marker_hold_flag.load();
}

JournalMarker JournalWriter::get_cur_producer_marker(){
    std::lock_guard<std::mutex> lck(producer_mtx);
    return cur_producer_marker;
}

int JournalWriter::update_producer_marker(const JournalMarker& marker){
    if(false == rpc_client->update_producer_marker(
            lease_client_->get_lease(),vol_attr_.vol_name(),marker)){
        LOG_ERROR << "update volume[" << vol_attr_.vol_name() << "] producer marker failed!";
        return -1;
    }
    return 0;
}

void JournalWriter::work()
{
    bool success = false;
    time_t start,end;
    shared_ptr<JournalEntry> entry = nullptr;
    uint64_t entry_size = 0;
    uint64_t write_size = 0;

    // update producer marker first when init, then the replicator
    // could replicate the data written during last crashed/restart time
    while(RepRole::REP_PRIMARY == vol_attr_.replicate_role() && running_flag){
        int res = get_next_journal();
        if(res != 0){
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        else{
            res = open_current_journal();
            SG_ASSERT(0 == res);
            SG_ASSERT(true == write_journal_header());

            // update cached producer marker
            std::lock_guard<std::mutex> lck(producer_mtx);
            cur_producer_marker.set_cur_journal(cur_lease_journal.second.journal());
            cur_producer_marker.set_pos(cur_journal_size);
            break;
        }
    }

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

        while(!success && (difftime(end,start) < conf_.journal_write_timeout))
        {
            // get journal file fd
            if(cur_file_ptr == nullptr
                || (entry_size + cur_journal_size) > conf_.journal_max_size){
                to_seal_current_journal();
                invalid_current_journal();
                int res = get_next_journal();
                if(res == 0){
                    res = open_current_journal();
                }
                if(res != 0){
                    time(&end);
                    continue;
                }
                SG_ASSERT(true == write_journal_header());

                // update cached producer marker
                std::lock_guard<std::mutex> lck(producer_mtx);
                cur_producer_marker.set_cur_journal(cur_lease_journal.second.journal());
                cur_producer_marker.set_pos(cur_journal_size);
            }

            // validate journal lease
            if (!lease_client_->check_lease_validity(cur_lease_journal.first))
            {
                // check lease valid failed
                invalid_current_journal();
                LOG_ERROR << "check lease validity result:false";
                time(&end);
                continue;
            }

            /*persist to journal file*/
            std::string journal_file = conf_.journal_mount_point + cur_lease_journal.second.path();
            off_t journal_off = cur_journal_size;
            write_size = entry->persist(cur_file_ptr, journal_off);
            if(write_size != entry_size)
            {
                LOG_ERROR << "write journal file: " << cur_lease_journal.second.path()
                          << " failed:" << strerror(errno);
                time(&end);
                continue;
            }
            /*clear message serialized data*/
            entry->clear_serialized_data();

            /*add to cache*/
            cacheproxy_->write(journal_file, journal_off, entry);

            // update journal offset
            cur_journal_size += write_size;
            success = true;

            /*todo: unify callback framework*/
            /*snapshot cmd synchronize as soon as possible*/
            if(entry->get_type() == SNAPSHOT_CREATE ||
               entry->get_type() == SNAPSHOT_DELETE ||
               entry->get_type() == SNAPSHOT_ROLLBACK){
                LOG_INFO << "journal write reply snapshot command";
                JournalMarker cur_write_mark;
                cur_write_mark.set_cur_journal(cur_lease_journal.second.journal());
                cur_write_mark.set_pos(journal_off);
                snapshot_proxy_->cmd_persist_notify(cur_write_mark); 
            }

            // update cached producer marker
            std::unique_lock<std::mutex> lck(producer_mtx);
            cur_producer_marker.set_cur_journal(cur_lease_journal.second.journal());
            cur_producer_marker.set_pos(cur_journal_size);
            written_size_since_last_update += write_size;
            lck.unlock();

            // seal the journal while created a snapshot for replication
            if(entry->get_type() == SNAPSHOT_CREATE){
                std::shared_ptr<SnapshotMessage> msg
                    = std::dynamic_pointer_cast<SnapshotMessage>(entry->get_message());
                if(msg->snap_scene() == huawei::proto::FOR_REPLICATION){
                    to_seal_current_journal();
                    invalid_current_journal();
                    LOG_INFO << "crerate snapshot for replication,seal current journal "
                        << cur_lease_journal.second.journal();
                }
            }
        }

        // to update producer marker if enough io were written
        if(producer_marker_hold_flag.load() == false
                && written_size_since_last_update >= conf_.journal_producer_written_size_threshold){
            producer_event.trigger_event();
            written_size_since_last_update = 0;
            LOG_DEBUG << "trigger to update producer marker:" << vol_attr_.vol_name();
        }

        // response to io scheduler
        if(entry->get_type() == IO_WRITE){
            send_reply(entry.get(),success);
        } else {
            ;
        }
    }
}

int JournalWriter::get_next_journal()
{
    {
        std::unique_lock<std::recursive_mutex> journal_uk(journal_mtx_);
        if(journal_queue.empty())
        {
            LOG_INFO << "journal_queue empty";
            get_writeable_journals(lease_client_->get_lease(), conf_.journal_limit);
        }

        if (journal_queue.empty())
        {
            return -1;
        }

        cur_lease_journal = journal_queue.front();
        journal_queue.pop();
        LOG_INFO << "journal_queue pop journal:" 
            << cur_lease_journal.second.journal()
            << ",path:" << cur_lease_journal.second.path();
    }
    cur_journal_size = 0;
    return 0;
}

int JournalWriter::open_current_journal()
{
    if (cur_lease_journal.second.journal() == "")
        return -1;
    std::string tmp = conf_.journal_mount_point + cur_lease_journal.second.path();
    cur_file_ptr = fopen(tmp.c_str(), "ab+");
    if(NULL == cur_file_ptr)
    {
        LOG_ERROR << "open journal file: " << cur_lease_journal.second.path()
            << " failed:" << strerror(errno);
        return -1;
    }

    idproxy_->add_file(tmp);
    return 0;
}

int JournalWriter::to_seal_current_journal()
{
    if(cur_lease_journal.first != "")
        seal_queue.push(cur_lease_journal);
    return 0;
}

int JournalWriter::close_current_journal_file()
{
    if(cur_file_ptr != nullptr)
        fclose(cur_file_ptr);
    cur_file_ptr = nullptr;
    return 0;
}

bool JournalWriter::write_journal_header()
{
    journal_file_header_t journal_header;
    if(fwrite(&journal_header,sizeof(journal_file_header_t),1,cur_file_ptr) != 1)
    {
        LOG_ERROR << "write journal header faied,journal:" << cur_lease_journal.second.path()
                  << "errno:" << strerror(errno);
        return false;
    }
    fflush(cur_file_ptr);
    cur_journal_size += sizeof(journal_file_header_t);
    return true;
}

bool JournalWriter::get_writeable_journals(const std::string& uuid,const int32_t limit)
{
    std::list<JournalElement> journals;
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
    if(!rpc_client->GetWriteableJournals(uuid,vol_attr_.vol_name(),tmp,journals))
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
    std::pair<std::string, JournalElement> tmp;
    std::list<std::string> journals;
    std::list<std::pair<std::string, JournalElement>> backup;

    std::unique_lock<std::mutex> lk(rpc_mtx_);
    std::unique_lock<std::mutex> seal_uk(seal_mtx_);

    while(!seal_queue.empty())
    {
        tmp = seal_queue.front();
        // only seal these journals with valid lease
        if (tmp.second.journal() != "" && uuid == tmp.first)
        {
            journals.push_back(tmp.second.journal());
            backup.push_back(tmp);
        }
        seal_queue.pop();
    }

    if (!journals.empty())
    {
        if(!rpc_client->SealJournals(uuid, vol_attr_.vol_name(), journals))
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

void JournalWriter::invalid_current_journal()
{
    JournalElement e;
    e.set_journal("");
    cur_lease_journal = std::make_pair("", e);
    cur_journal_size = 0;
    close_current_journal_file();
}

VolumeAttr& JournalWriter::get_vol_attr()
{
    return vol_attr_;
}

}


