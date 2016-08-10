#include "proxy.h"

/*journal write call*/
void Cproxy::write(uint64_t log_seq, string log_file, 
                   off_t log_off, shared_ptr<log_header_t> log_entry)
{
    /*type cast*/
    log_header_t* logh = log_entry.get();

    //?
    for(int i=0; i < logh->count; i++)
    {
        off_t  off = logh->off_len[i].offset;
        size_t len = logh->off_len[i].length; 

        if(bcache->isfull(len)){
            /*cache memory over threshold, cache point to log file location*/
            Bkey bkey(off, len, log_seq);
            Jkey jkey(log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off));
            jcache->add(jkey, v);
            bcache->add(bkey, v);
        } else {
            /*cache memory in threshold, cache point to log entry in memory*/
            Bkey bkey(off, len, log_seq);
            Jkey jkey(log_seq);
            shared_ptr<CEntry> v(new CEntry(log_seq, log_file, log_off, log_entry));
            jcache->add(jkey, v);
            bcache->add(bkey, v);
        }
    }

}

/*message dispatch call*/
int Cproxy::read(off_t off, size_t len, char* buf)
{
    /*read from bcache*/
    return bcache->read(off, len, buf);
}
    
/*journal replayer call*/
bool Cproxy::pop(shared_ptr<CEntry>& item)
{
    /*pop item from jcache*/
    return jcache->pop(item);
}

bool Cproxy::free(shared_ptr<CEntry>& item)
{
    CEntry* centry = item.get();

    /*delete from jcache*/

    /*delete from bcache*/

    return true;
}

void Cproxy::trace()
{
    if(jcache){
        jcache->trace();
    }
    if(bcache){
        bcache->trace();
    }
}
