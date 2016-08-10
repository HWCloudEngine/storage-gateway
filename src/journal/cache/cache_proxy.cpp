#include "cache_proxy.h"

/*journal write call*/
void CacheProxy::write(IoVersion log_seq, string log_file, 
                   off_t log_off, shared_ptr<ReplayEntry> log_entry)
{
    /*type cast*/
    ReplayEntry*  re = log_entry.get();
    log_header_t* logh = (log_header_t*)re->data();

    /*todo: batch io case, may reconsider how to sequence each io*/
    for(int i=0; i < logh->count; i++)
    {
        off_t  off = logh->off_len[i].offset;
        size_t len = logh->off_len[i].length; 

        if(bcache->isfull(len)){
            /*cache memory over threshold, cache point to log file location*/
            Bkey bkey(off, len, log_seq);
            Jkey jkey(log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off));
            bool ret = jcache->add(jkey, v);
            if(!ret){
                jcache->update(jkey, v);
            }
            ret = bcache->add(bkey, v);
            if(!ret){
                bcache->update(bkey, v);
            }

        } else {
            /*cache memory in threshold, cache point to log entry in memory*/
            Bkey bkey(off, len, log_seq);
            Jkey jkey(log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off, log_entry));
            
            bool ret = jcache->add(jkey, v);
            if(!ret){
                jcache->update(jkey, v);
            }
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
    return bcache->read(off, len, buf);
}
    
/*replayer relevant*/
Jkey CacheProxy::top()
{
    return jcache->top();
}

shared_ptr<CEntry> CacheProxy::retrieve(const Jkey& key)
{
    return jcache->get(key);
}

bool CacheProxy::reclaim(shared_ptr<CEntry> data)
{
    /*delete from bcache*/
    CEntry* ce = data.get();
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

    /*delete from jcache*/
    Jkey key(log_seq);
    jcache->del(key);

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
