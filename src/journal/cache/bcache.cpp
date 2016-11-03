#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <set>
#include <stack>
#include <algorithm>
#include "bcache.h"
#include "common.h"

#define MIN(a, b) ((a) < (b) ? (a) :(b))

bool operator<(const Bkey& a, const Bkey& b)
{
    //seq
    if(a.m_off != b.m_off){
        return a.m_off < b.m_off;
    } else {
        if(a.m_len != b.m_len){
            return a.m_len < b.m_len;
        } else {
            return a.m_seq < b.m_seq;
        } 
    }
    
    /*here return false mean a == b*/
    return false;
}

ostream& operator<<(ostream& cout, const Bkey& key)
{
    LOG_INFO << "[off:" << key.m_off << " len:" << key.m_len \
             << " seq:" << key.m_seq << "]";

    return cout;
}

bool BkeySeqCompare(const Bkey& a, const Bkey& b)
{
    return a.m_seq < b.m_seq;
}

bool BkeyOffsetCompare(const Bkey& a, const Bkey& b)
{
    return a.m_off < b.m_off;
}

bool Bcache::add(Bkey key, shared_ptr<CEntry> value)
{
    /*write lock */
    WriteLock write_lock(m_mutex);
    std::pair<std::map<Bkey, shared_ptr<CEntry>>::iterator, bool> ret;
    ret = m_bcache.insert(std::pair<Bkey, shared_ptr<CEntry>>(key, value));
    if(ret.second == false){
        LOG_ERROR << "add bkey:" << key << "failed";
        return false;  
    }
    return true;
}

shared_ptr<CEntry> Bcache::get(Bkey key)
{
    /*read lock*/
    ReadLock read_lock(m_mutex);
    auto it = m_bcache.find(key);
    if(it != m_bcache.end()){
        return it->second;
    }
    LOG_ERROR << "get key: " << key  << "failed";
    return nullptr;
}

bool Bcache::update(Bkey key, shared_ptr<CEntry> value)
{
    /*write lock*/
    WriteLock write_lock(m_mutex);
    auto it = m_bcache.find(key);
    if(it != m_bcache.end()){
        m_bcache.erase(it);
    }
    std::pair<std::map<Bkey, shared_ptr<CEntry>>::iterator, bool> ret;
    ret = m_bcache.insert(std::pair<Bkey, shared_ptr<CEntry>>(key, value));
    if(ret.second == false){
        LOG_ERROR << "update key: " << key << "failed";
        return false;  
    }
    return true;
}

bool Bcache::del(Bkey key)
{
    /*write lock*/
    WriteLock write_lock(m_mutex);
    auto it = m_bcache.find(key);
    if(it != m_bcache.end()){
        m_bcache.erase(it);
        return true;
    }
    LOG_ERROR << "del key:" << key << "failed";
    return false;
}

bcache_itor_t Bcache::_data_lower_bound(off_t offset, size_t length)
{
    bcache_itor_t p = m_bcache.lower_bound(Bkey(offset, length, IoVersion(0,0)));

    while(true){
        if(p == m_bcache.begin()){
            break;
        }
        p--;
        if(p->first.m_off + p->first.m_len <= offset){
            p++;
            break;
        }
    } 

    return p;
}

void Bcache::_find_hit_region(off_t offset, size_t length, 
                              bcache_map_t& region_hits) 
{
    bcache_itor_t p = _data_lower_bound(offset, length);
    while(p != m_bcache.end()){
        if(p->first.m_off >= offset + length)
            break;
        region_hits.insert(pair<Bkey, shared_ptr<CEntry>>(p->first, p->second));
        p++;
    }
}

void Bcache::_merge_hit_region(vector<Bkey> bkeys, vector<Bkey>& merged_bkeys)
{
    if(bkeys.empty()){
        return;
    }

    sort(bkeys.begin(), bkeys.end(), BkeyOffsetCompare); 

    stack<Bkey> s;
    s.push(bkeys[0]);
    for(int i = 1; i < bkeys.size(); i++)
    {
        Bkey top = s.top();
        if(top.m_off + top.m_len < bkeys[i].m_off){
            s.push(bkeys[i]);
        } else if ((top.m_off+top.m_len) < (bkeys[i].m_off + bkeys[i].m_len)){
            top.m_len = bkeys[i].m_off+bkeys[i].m_len - top.m_off;
            s.pop();
            s.push(top);
        }
    }
   
    while(!s.empty()){
        Bkey t = s.top();
        s.pop();
        merged_bkeys.push_back(t);
    }

    sort(merged_bkeys.begin(), merged_bkeys.end(), BkeyOffsetCompare);
}

void Bcache::_find_miss_region(off_t off, size_t len, 
                               const vector<Bkey>& merged_bkeys, 
                               vector<Bkey>& miss_bkeys)
{
    off_t  cur = off;
    size_t left = len;

    vector<Bkey>::const_iterator p = merged_bkeys.begin();
    while(left > 0)
    {
        if(p == merged_bkeys.end()){
            Bkey bkey(cur, left, IoVersion(0,0));
            miss_bkeys.push_back(bkey);
            cur += left;
            break;
        }
        
        if(cur < p->m_off){
            off_t  next = p->m_off;
            size_t len  = MIN(next-cur, left);
            Bkey key(cur, len, IoVersion(0,0));
            miss_bkeys.push_back(key);

            cur += min(left, len);
            left -= min(left, len);
            continue; 
        } else {
            size_t lenfromcur = MIN(p->m_off + p->m_len - cur, left);
            cur += lenfromcur;
            left -= lenfromcur;
            p++;
            continue;
        }
    }

}

int Bcache::_cache_hit_read(off_t off, size_t len, char* buf, 
                            const vector<Bkey>& hit_keys, 
                            bcache_map_t& hit_cache_snapshot)
{
    LOG_DEBUG << "hit read start ";
    for(int i = 0; i < hit_keys.size(); i++){
        Bkey k = hit_keys[i];

        shared_ptr<CEntry> v;
        auto it = hit_cache_snapshot.find(k);
        if(it == hit_cache_snapshot.end()){
            LOG_ERROR << "cache snapshot find failed";
            break;
        }
        v = it->second;
         
        char*  pdst = NULL;
        size_t pdst_len  = 0;  
        off_t  pdst_off  = 0;
       
        if(off > k.m_off){
            pdst_off = off;
            pdst_len = MIN(len, k.m_off+k.m_len - off);
        } else {
            pdst_off  = k.m_off;
            pdst_len  = MIN(k.m_len, off+len-k.m_off);
        }
       
        if(pdst_off < off){
            LOG_ERROR << "pdst_off < off"; 
            continue;
        }
        pdst = buf + (pdst_off-off);
       
        shared_ptr<ReplayEntry> log_head = nullptr;
        uint8_t cache_type = v->get_cache_type();
        if(cache_type == CEntry::IN_MEM){
            //in memory
            //todo: one log header has many io
            LOG_DEBUG << "hit read from memory";
            log_head = v->get_log_entry(); 
            LOG_DEBUG << "hit read from memory ok";
        } else if(cache_type == CEntry::IN_LOG){
            //in log  
            //todo: one log header has many io
            LOG_DEBUG << "hit read from log";
            string log_file = v->get_log_file();
            off_t  log_off  = v->get_log_off();
            IReadFile* rfile = new SyncReadFile(log_file, log_off, false);
            rfile->open();
            rfile->read_entry(log_off, m_buffer_pool, log_head);
            rfile->close();
            delete rfile;
            LOG_DEBUG << "hit read from log ok ";
        } else {
            //assert(0);
        }

        log_header_t* plog_head = (log_header_t*)log_head->data();
        char* psrc = (char*)plog_head + plog_head->count*sizeof(off_len_t);
        psrc += pdst_off-k.m_off;
        memcpy(pdst, psrc, pdst_len);
    }

    LOG_DEBUG << "hit read ok ";
    return 0;
}

int Bcache::_cache_miss_read(off_t off, size_t len, char* buf, 
                             const vector<Bkey>& miss_keys)
{
    int blk_fd = open(m_blkdev.c_str(), O_RDONLY|O_CREAT|O_TRUNC, 0777);
    if(blk_fd == -1){
        LOG_ERROR << "miss_read open:" <<m_blkdev << "failed";
        return -1;
    }

    LOG_DEBUG << "miss read start ";
    for(auto it : miss_keys){
        Bkey k = it;

        char*  pbuf     = NULL;
        size_t pbuf_len = 0;  
        off_t  poffset  = 0;

        if(off > k.m_off){
            if(off > k.m_off + k.m_len){
                continue;
            } else {
                poffset = off;
                pbuf_len = MIN(len, k.m_off+k.m_len - off);
            }
        } else if(k.m_off > off + len){
            continue;
        } else {
            poffset = k.m_off;
            pbuf_len = MIN(k.m_len, off+len-k.m_off);
        }

        pbuf = buf+(poffset-off);
        size_t left = pbuf_len; 
        size_t read = 0;

        while(left > 0){
            int ret = pread(blk_fd, pbuf+read, left, poffset+read);
            if(ret == -1 || ret == 0){
                LOG_ERROR << "miss_read pread failed errno:" << errno 
                          << "ret:"<< ret;
                break;
            }
            left -= ret;
            read += ret;
        }
    } 
    
    if(blk_fd != -1){
        close(blk_fd);
    }

    LOG_DEBUG << "miss read ok";
    return 0;
}

int Bcache::read(off_t off, size_t len, char* buf)
{
    /*snapshot store which bkey and centry pair cache hit*/
    bcache_map_t region_hits;

    /*find cache hit  region, no seq warrant*/
    {
        ReadLock read(m_mutex);
        _find_hit_region(off, len, region_hits);
    }
   
    if(!region_hits.empty()){
        LOG_DEBUG << "read region hit ";
        for(auto it : region_hits){
            LOG_DEBUG << "\t" << it.first.m_off 
                      << "--" << it.first.m_len  \
                      << "--" << it.first.m_seq << endl; 
        }

        /*cache hit region, make seq warrant */
        vector<Bkey> order_hit_keys;
        for(auto it : region_hits){
            order_hit_keys.push_back(it.first);
        }
        sort(order_hit_keys.begin(), order_hit_keys.end(), BkeySeqCompare);

        LOG_DEBUG << "read region hit order hit keys" << endl;
        for(auto it : order_hit_keys){
            LOG_DEBUG << "\t" << it.m_off 
                      << "--" << it.m_len  
                      << "--" << it.m_seq; 
        }

        /*temporay cache hit merge for latter step*/
        vector<Bkey> merged_keys;
        _merge_hit_region(order_hit_keys, merged_keys);

        LOG_DEBUG << "read region merged keys" << endl;
        for(auto it : merged_keys){
            LOG_DEBUG << "\t" << it.m_off << "******" << it.m_len;
        }

        /*find miss region*/
        vector<Bkey> order_miss_keys;
        _find_miss_region(off, len, merged_keys, order_miss_keys);

        LOG_DEBUG << "read region miss keys";
        for(auto it: order_miss_keys){
            LOG_DEBUG << "\t" << it.m_off << "%%%%%%" << it.m_len;
        }

        /*read from cache*/
        if(!order_hit_keys.empty()){
            _cache_hit_read(off, len, buf, order_hit_keys, region_hits);
        }

        /*read from device*/
        if(!order_miss_keys.empty()){
            _cache_miss_read(off, len, buf, order_miss_keys);
        }

    } else {
        LOG_DEBUG << "read region no hit ";
        vector<Bkey> order_miss_keys;
        order_miss_keys.push_back(Bkey(off, len, IoVersion(0,0)));
        _cache_miss_read(off, len, buf, order_miss_keys);
    }
        
    return 0;
}

void Bcache::trace()
{
    for(auto it : m_bcache)
    {
        LOG_INFO << "\t" << "(" << it.first.m_off << " "  \
                 << it.first.m_len << " "  \
                 << it.first.m_seq << ")"  \
                 <<"[" << it.second.use_count() << "]";
    }
}
