/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    backup_proxy.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup entry
*  
*************************************************/
#include "cache_proxy.h"
#include "rpc/message.pb.h"
using google::protobuf::Message;
using huawei::proto::WriteMessage;
using huawei::proto::DiskPos;

CacheProxy::CacheProxy(string blk_dev, shared_ptr<IDGenerator> id_maker) {
    blkdev = blk_dev;
    idproc = id_maker;
    total_mem_size = 0;
    start_cache_evict_thr();
    jcache = new Jcache(blk_dev);
    bcache = new Bcache(blk_dev);
    LOG_INFO << "CacheProxy create";
}

CacheProxy::~CacheProxy() {
    LOG_INFO << "CacheProxy destroy";
    stop_cache_evict_thr();
    delete bcache;
    delete jcache;
    LOG_INFO << "CacheProxy destroy ok";
}

/*journal write call*/
void CacheProxy::write(string journal_file, off_t journal_off,
                       shared_ptr<JournalEntry> journal_entry) {
    int ret = 0;
    journal_event_type_t type = journal_entry->get_type();
    if (IO_WRITE == type) {
        shared_ptr<Message> message = journal_entry->get_message();
        shared_ptr<WriteMessage> write_message = dynamic_pointer_cast<WriteMessage>(message);
        IoVersion io_seq  = idproc->get_version(journal_file);
        int pos_num = write_message->pos_size();

        for (int i=0; i < pos_num; i++) {
            DiskPos* pos = write_message->mutable_pos(i);
            off_t    off = pos->offset();
            size_t   len = pos->length();
            LOG_INFO << " write jfile:" << journal_file  << " joff:" << journal_off 
                     << " io_seq:" << io_seq << " blk_off:" << off << " blk_len:" << len;
            if (isfull(len)) {
                /*trigger bcache evict*/
                trigger_cache_evict();
                /*cache memory over threshold, cache point to journal file location*/
                Bkey bkey(off, len, io_seq);
                shared_ptr<CEntry> v(new CEntry(io_seq, off, len, journal_file,journal_off));
                unique_lock<mutex> lock_region(cache_lock);
                jcache->push(v);
                ret = bcache->add(bkey, v);
                if (!ret) {
                    bcache->update(bkey, v);
                }
                total_mem_size += v->get_mem_size();
            } else {
                /*cache memory in threshold, cache point to journal entry in memory*/
                Bkey bkey(off, len, io_seq);
                shared_ptr<CEntry> v(new CEntry(io_seq, off, len, journal_file, journal_off,
                                                journal_entry));
                unique_lock<mutex> lock_region(cache_lock);
                jcache->push(v);
                ret = bcache->add(bkey, v);
                if (!ret) {
                    bcache->update(bkey,v);
                }
                total_mem_size += v->get_mem_size();
            }
        }
    } else {
        /*other type message, only push to jcache*/
        IoVersion io_seq  = idproc->get_version(journal_file);
        shared_ptr<CEntry> v(new CEntry(io_seq, 0, 0, journal_file, journal_off,
                                        journal_entry));
        unique_lock<mutex> lock_region(cache_lock);
        jcache->push(v);
    }
}

/*message dispatch call*/
int CacheProxy::read(off_t off, size_t len, char* buf) {
    /*read from bcache*/
    LOG_INFO << "read off:" << off << " len:" << len;
    return bcache->read(off, len, buf);
}
/*replayer relevant*/
shared_ptr<CEntry> CacheProxy::pop() {
    unique_lock<mutex> lock_region(cache_lock);
    return jcache->pop();
}

bool CacheProxy::reclaim(shared_ptr<CEntry> entry) {
    unique_lock<mutex> lock_region(cache_lock);
    /*delete from bcache*/
    CEntry* ce = entry.get();
    LOG_INFO << " reclaim  entry" << " io_seq:"  << entry->get_io_seq()
             << " blk_off:" << entry->get_blk_off()
             << " blk_len:" << entry->get_blk_len();

    if (ce->get_cache_type() == CEntry::IN_MEM) {
        /*todo other message*/
        shared_ptr<JournalEntry> journal_entry = ce->get_journal_entry();
        journal_event_type_t type = journal_entry->get_type();
        if (type == IO_WRITE) {
            shared_ptr<Message> message = journal_entry->get_message();
            shared_ptr<WriteMessage> io_message = dynamic_pointer_cast<WriteMessage>(message);
            IoVersion io_seq   = ce->get_io_seq();
            int pos_num = io_message->pos_size();
            for (int i=0; i < pos_num; i++) {
                DiskPos* pos = io_message->mutable_pos(i);
                off_t    off = pos->offset();
                size_t   len = pos->length();
                Bkey key(off, len, io_seq);
                bcache->del(key);
            }
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

bool CacheProxy::isfull(size_t cur_io_size) {
    return true;
    if (total_mem_size + cur_io_size > MAX_CACHE_LIMIT) {
        return true;
    }
    return false;
}

void CacheProxy::start_cache_evict_thr() {
    evict_run = true;
    evict_thread = new thread(bind(&CacheProxy::cache_evict_work, this));
}

void CacheProxy::stop_cache_evict_thr() {
    evict_run = false;
    LOG_INFO << "stop cache evict thr";
    evict_cond.notify_all();
    evict_thread->join();
    LOG_INFO << "stop cache evict thr ok";
    delete evict_thread;
}

void CacheProxy::trigger_cache_evict() {
    unique_lock<mutex> lock(evict_lock);
    evict_cond.notify_all();
    LOG_INFO << "trigger cache evict";
}

void CacheProxy::cache_evict_work() {
    while (evict_run) {
        unique_lock<mutex> evict_lock_region(evict_lock);
        evict_cond.wait(evict_lock_region);
        int already_evit_size = 0;
        
        {
            unique_lock<mutex> cache_lock_region(cache_lock);
            /*evict should start from the oldest entry in jcache*/
            Jcache::Iterator it = jcache->begin();
            for (; it != jcache->end(); it++) {
                shared_ptr<CEntry> centry = it.second();
                int centry_mem_size = centry->get_mem_size();
                /*entry is in journal file*/
                if (centry->get_cache_type() == CEntry::IN_JOURANL) {
                    continue;
                }
                /*entry is control command*/
                if (centry->get_blk_off() == 0 && centry->get_blk_len() == 0) {
                    continue;
                }
                /*CEntry point to journal file instead of ReplayEntry in memory*/
                centry->set_cache_type(CEntry::IN_JOURANL);
                centry->get_journal_entry().reset();

                LOG_INFO << "update key" << " off:" << centry->get_blk_off()
                    << " len:" << centry->get_blk_len() 
                    << " seq:" << centry->get_io_seq();

                /*update bcache Centry*/
                Bkey bkey(centry->get_blk_off(), centry->get_blk_len(),
                        centry->get_io_seq());
                bool ret = bcache->update(bkey, centry);

                /*update jcache Centry*/
                Jkey jkey(centry->get_io_seq());
                ret &= jcache->update(jkey, centry);

                /*both update ok*/
                if (ret) {
                    already_evit_size += centry_mem_size;
                    total_mem_size -= centry_mem_size;
                }

                if (already_evit_size >= CACHE_EVICT_SIZE) {
                    break;
                }
            }
        }
    }
}

void CacheProxy::trace() {
    if (jcache) {
        jcache->trace();
    }
    if (bcache) {
        bcache->trace();
    }
}
