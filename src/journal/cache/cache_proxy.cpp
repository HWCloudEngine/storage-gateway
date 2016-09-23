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
    
    LOG_DEBUG << "write file:" << log_file << " off:" << log_off;

    /*todo: batch io case, may reconsider how to sequence each io*/
    for(int i=0; i < logh->count; i++)
    {
        off_t  off = logh->off_len[i].offset;
        size_t len = logh->off_len[i].length; 
        
        if(bcache->isfull(len)){
            /*cache memory over threshold, cache point to log file location*/
            Bkey bkey(off, len, log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off, off, len));
            jcache->push(v);
            ret = bcache->add(bkey, v);
            if(!ret){
                bcache->update(bkey, v);
            }
        } else {
            /*cache memory in threshold, cache point to log entry in memory*/
            Bkey bkey(off, len, log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off, log_entry));
            jcache->push(v);
            ret = bcache->add(bkey, v);
            if(!ret){
                bcache->update(bkey,v);
            }
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
    if(ce->get_cache_type() == CEntry::IN_MEM){
        ReplayEntry*  re   = ce->get_log_entry().get();
        log_header_t* logh = (log_header_t*)re->data();
        IoVersion log_seq = ce->get_log_seq();
        for(int i=0; i < logh->count; i++)
        {
            off_t  off = logh->off_len[i].offset;
            size_t len = logh->off_len[i].length; 

            Bkey key(off, len, log_seq);
            bcache->del(key);
        }
    } else {
        IoVersion log_seq = ce->get_log_seq();
        off_t off  = ce->get_blk_off();
        size_t len = ce->get_blk_len();
        Bkey key(off, len, log_seq);
        bcache->del(key);
    }

   return true;
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
