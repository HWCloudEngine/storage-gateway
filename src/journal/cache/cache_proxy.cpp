#include "cache_proxy.h"

/*journal write call*/
void CacheProxy::write(string log_file, off_t log_off, 
                       shared_ptr<ReplayEntry> log_entry)
{
    int ret = 0;
    /*type cast*/
    ReplayEntry*  re   = log_entry.get();
    log_header_t* logh = (log_header_t*)re->data();
    
    IoVersion log_seq = idproc->get_version(log_file);

    LOG_DEBUG << " write log_file:" << log_file << " log_off:" << log_off;

    /*todo: batch io case, may reconsider how to sequence each io*/
    for(int i=0; i < logh->count; i++)
    {
        off_t  off = logh->off_len[i].offset;
        size_t len = logh->off_len[i].length; 
        
        if(isfull(len)){

            /*trigger bcache evict, only reduce bcache map size*/
            bcache_evict();

            /*cache memory over threshold, cache point to log file location*/
            Bkey bkey(off, len, log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off, off, len));
            jcache->push(v);
            ret = bcache->add(bkey, v);
            if(!ret){
                bcache->update(bkey, v);
            }
            total_mem_size += v->get_mem_size();
        } else {
            /*cache memory in threshold, cache point to log entry in memory*/
            Bkey bkey(off, len, log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off, off, len, log_entry));
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
    LOG_DEBUG << "read off:" << off << " len:" << len ;
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
    
    LOG_INFO << " reclaim blk_off:" << entry->get_blk_off()
             << " blk_len:" << entry->get_blk_len()
             << " seq:" << entry->get_log_seq();

    if(ce->get_cache_type() == CEntry::IN_MEM){
        ReplayEntry*  re   = ce->get_log_entry().get();
        log_header_t* logh = (log_header_t*)re->data();
        IoVersion log_seq  = ce->get_log_seq();
        for(int i=0; i < logh->count; i++)
        {
            off_t  off = logh->off_len[i].offset;
            size_t len = logh->off_len[i].length; 
            Bkey key(off, len, log_seq);
            bcache->del(key);
       }
    } else {
        IoVersion log_seq = ce->get_log_seq();
        off_t  off  = ce->get_blk_off();
        size_t len  = ce->get_blk_len();
        Bkey key(off, len, log_seq);
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

void CacheProxy::bcache_evict()
{
    int already_evit_size = 0;
    BlockingQueue<shared_ptr<CEntry>>& jcache_queue = jcache->get_queue() ;
    for(int i = 0; i < jcache_queue.entry_number(); i++){
        shared_ptr<CEntry> centry_sptr = jcache_queue[i];
        CEntry* centry_ptr  = centry_sptr.get();
        int centry_mem_size = centry_ptr->get_mem_size();

        Bkey evit_key(centry_ptr->get_blk_off(), 
                      centry_ptr->get_blk_len(), 
                      centry_ptr->get_log_seq());
        
        LOG_INFO << "evict off:" << centry_ptr->get_blk_off() \
                 << " len:" << centry_ptr->get_blk_len() \
                 << " seq:" << centry_ptr->get_log_seq();

        /*only delete from bcache, reduce bcache map size*/
        bool ret = bcache->del(evit_key);
        if(ret){
            already_evit_size += centry_mem_size;
        }
       
        if(already_evit_size >= CACHE_EVICT_SIZE)
            break;
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
