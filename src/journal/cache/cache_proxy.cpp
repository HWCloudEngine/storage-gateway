#include "cache_proxy.h"

/*journal write call*/
void CacheProxy::write(string journal_file, off_t journal_off, 
                       shared_ptr<ReplayEntry> journal_entry)
{
    int ret = 0;
    /*type cast*/
    ReplayEntry*  re   = journal_entry.get();
    log_header_t* logh = (log_header_t*)re->data();
    IoVersion     io_seq  = idproc->get_version(journal_file);

    LOG_INFO << " write journal_file:" << journal_file 
             << " journal_off:" << journal_off;

    /*todo: batch io case, may reconsider how to sequence each io*/
    for(int i=0; i < logh->count; i++)
    {
        off_t  off = logh->off_len[i].offset;
        size_t len = logh->off_len[i].length; 
        
        if(isfull(len)){
            /*trigger bcache evict*/
            trigger_cache_evict();

            /*cache memory over threshold, cache point to journal file location*/
            Bkey bkey(off, len, io_seq);
            shared_ptr<CEntry> v(new CEntry(io_seq, off, len, 
                                            journal_file,journal_off));
            jcache->push(v);
            ret = bcache->add(bkey, v);
            if(!ret){
                bcache->update(bkey, v);
            }
            total_mem_size += v->get_mem_size();
        } else {
            /*cache memory in threshold, cache point to journal entry in memory*/
            Bkey bkey(off, len, io_seq);
            shared_ptr<CEntry> v(new CEntry(io_seq, off, len, 
                                            journal_file, journal_off, 
                                            journal_entry));
            jcache->push(v);
            ret = bcache->add(bkey, v);
            if(!ret){
                bcache->update(bkey,v);
            }
            total_mem_size += v->get_mem_size();
       }
    }
}

/*message dispatch call*/
int CacheProxy::read(off_t off, size_t len, char* buf)
{
    /*read from bcache*/
    LOG_INFO << "read off:" << off << " len:" << len ;
    return bcache->read(off, len, buf);
}
    
/*replayer relevant*/
shared_ptr<CEntry> CacheProxy::pop()
{
    return jcache->pop();
}

bool CacheProxy::reclaim(shared_ptr<CEntry> entry)
{
    /*delete from bcache*/
    CEntry* ce = entry.get();
    
    LOG_INFO << " reclaim  entry"
             << " io_seq:"  << entry->get_io_seq()
             << " blk_off:" << entry->get_blk_off()
             << " blk_len:" << entry->get_blk_len();

    if(ce->get_cache_type() == CEntry::IN_MEM){
        ReplayEntry*  re   = ce->get_journal_entry().get();
        log_header_t* logh = (log_header_t*)re->data();
        IoVersion io_seq   = ce->get_io_seq();
        for(int i=0; i < logh->count; i++)
        {
            off_t  off = logh->off_len[i].offset;
            size_t len = logh->off_len[i].length; 
            Bkey key(off, len, io_seq);
            bcache->del(key);
       }
    } else {
        IoVersion io_seq = ce->get_io_seq();
        off_t  off  = ce->get_blk_off();
        size_t len  = ce->get_blk_len();
        Bkey key(off, len, io_seq);
        bcache->del(key);
    }

    /*here both bcache and jcache already delete CEntry*/
    total_mem_size -= ce->get_mem_size();
    return true;
}

bool CacheProxy::isfull(size_t cur_io_size)
{
    LOG_INFO << " total_mem_size: " << total_mem_size 
             << " cur_io_size:" << cur_io_size;

    if(total_mem_size + cur_io_size > MAX_CACHE_LIMIT)
        return true;
    return false;
}

void CacheProxy::start_cache_evict_thr()
{
    evict_run = true;
    evict_thread = new thread(bind(&CacheProxy::cache_evict_work, this));
}

void CacheProxy::stop_cache_evict_thr()
{
    evict_run = false;
    unique_lock<mutex> lock(evict_lock);
    evict_cond.notify_all();
    evict_thread->join();
    delete evict_thread;
}

void CacheProxy::trigger_cache_evict()
{
    unique_lock<mutex> lock(evict_lock);
    evict_cond.notify_all();
}

void CacheProxy::cache_evict_work()
{
    while(evict_run){
        unique_lock<mutex> lock(evict_lock); 
        evict_cond.wait(lock); 
        int already_evit_size = 0;
        BlockingQueue<shared_ptr<CEntry>>& jcache_queue = jcache->get_queue() ;

        /*evict should start from the oldest entry in jcache*/
        for(int i = 0; i < jcache_queue.entry_number(); i++){
            shared_ptr<CEntry>& centry = jcache_queue[i];
            int centry_mem_size = centry->get_mem_size();

            if(centry->get_cache_type() == CEntry::IN_JOURANL){
                continue;
            }

            Bkey update_key(centry->get_blk_off(), 
                    centry->get_blk_len(), 
                    centry->get_io_seq());

            LOG_INFO << "update key"
                     << " off:" << centry->get_blk_off() 
                     << " len:" << centry->get_blk_len() 
                     << " seq:" << centry->get_io_seq();

            /*update jcache CEntry*/
            /*CEntry point to journal file instead of ReplayEntry in memory*/
            centry->set_cache_type(CEntry::IN_JOURANL);
            centry->get_journal_entry().reset();

            /*update bcache Centry*/
            bool ret = bcache->update(update_key, centry);
            if(ret){
                already_evit_size += centry_mem_size;
            }

            if(already_evit_size >= CACHE_EVICT_SIZE){
                break;
            }
        }
    }
}

void CacheProxy::trace()
{
    if(jcache){
        jcache->trace();
    }
    if(bcache){
        bcache->trace();
    }
}
