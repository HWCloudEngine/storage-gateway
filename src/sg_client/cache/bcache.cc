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
#include "common/config_option.h"
#include "rpc/message.pb.h"

using google::protobuf::Message;
using huawei::proto::WriteMessage;

Bkey::Bkey(off_t off, size_t len, IoVersion seq) {
    m_off = off;
    m_len = len;
    m_seq = seq;
}

Bkey::Bkey(const Bkey& other) {
    m_off = other.m_off;
    m_len = other.m_len;
    m_seq = other.m_seq;
}

Bkey::Bkey(Bkey&& other) {
    m_off = std::move(other.m_off);
    m_len = std::move(other.m_len);
    m_seq = std::move(other.m_seq);
}

Bkey& Bkey::operator=(const Bkey& other) {
    if (this != &other) {
        m_off = other.m_off;
        m_len = other.m_len;
        m_seq = other.m_seq;
    }
    return *this;
}

Bkey& Bkey::operator=(Bkey&& other) {
    if (this != &other) {
        m_off = std::move(other.m_off);
        m_len = std::move(other.m_len);
        m_seq = std::move(other.m_seq);
    }
    return *this;
}

bool operator<(const Bkey& a, const Bkey& b) {
    //seq
    if (a.m_off != b.m_off) {
        return a.m_off < b.m_off;
    } else {
        if (a.m_len != b.m_len) {
            return a.m_len < b.m_len;
        } else {
            return a.m_seq < b.m_seq;
        }
    }
    /*here return false mean a == b*/
    return false;
}

ostream& operator<<(ostream& cout, const Bkey& key) {
    LOG_INFO << "[off:" << key.m_off << " len:" << key.m_len \
             << " seq:" << key.m_seq << "]";
    return cout;
}

bool BkeySeqCompare(const Bkey& a, const Bkey& b) {
    return a.m_seq < b.m_seq;
}

bool BkeyOffsetCompare(const Bkey& a, const Bkey& b) {
    return a.m_off < b.m_off;
}

Bcache::Bcache(string bdev) {
    m_blkdev = bdev;
    Env::instance()->create_access_file(m_blkdev, true, &m_blkfile);
    LOG_INFO << "Bcache create";
}

Bcache::~Bcache() {
    m_bcache.clear();
    LOG_INFO << "Bcache destroy";
}

bool Bcache::add(Bkey key, shared_ptr<CEntry> value) {
    /*write lock */
    WriteLock write_lock(m_mutex);
    auto ret = m_bcache.insert({key, value});
    if (ret.second == false) {
        LOG_ERROR << "add bkey:" << key << "failed";
        return false;
    }
    return true;
}

shared_ptr<CEntry> Bcache::get(Bkey key) {
    /*read lock*/
    ReadLock read_lock(m_mutex);
    auto it = m_bcache.find(key);
    if (it != m_bcache.end()) {
        return it->second;
    }
    LOG_ERROR << "get key: " << key  << "failed";
    return nullptr;
}

bool Bcache::update(Bkey key, shared_ptr<CEntry> value) {
    /*write lock*/
    WriteLock write_lock(m_mutex);
    auto it = m_bcache.find(key);
    if (it != m_bcache.end()) {
        m_bcache.erase(it);
    }

    auto ret = m_bcache.insert({key, value});
    if (ret.second == false) {
        LOG_ERROR << "update key: " << key << "failed";
        return false;
    }
    return true;
}

bool Bcache::del(Bkey key) {
    /*write lock*/
    WriteLock write_lock(m_mutex);
    auto it = m_bcache.find(key);
    if (it != m_bcache.end()) {
        m_bcache.erase(it);
        return true;
    }
    LOG_ERROR << "del key:" << key << "failed";
    return false;
}

bcache_itor_t Bcache::_data_lower_bound(off_t offset, size_t length) {
    bcache_itor_t p = m_bcache.lower_bound(Bkey(offset, length, IoVersion(0, 0)));

    while (true) {
        if (p == m_bcache.begin()) {
            break;
        }
        p--;
        if (p->first.m_off + p->first.m_len <= offset) {
            p++;
            break;
        }
    }

    return p;
}

void Bcache::_find_hit_region(off_t offset, size_t length,
                              bcache_map_t& region_hits) {
    bcache_itor_t p = _data_lower_bound(offset, length);
    while (p != m_bcache.end()) {
        if (p->first.m_off >= offset + length)
            break;
        region_hits.insert(pair<Bkey, shared_ptr<CEntry>>(p->first, p->second));
        p++;
    }
}

void Bcache::_merge_hit_region(vector<Bkey> bkeys, vector<Bkey>& merged_bkeys) {
    if (bkeys.empty()) {
        return;
    }

    sort(bkeys.begin(), bkeys.end(), BkeyOffsetCompare);

    stack<Bkey> s;
    s.push(bkeys[0]);
    for (int i = 1; i < bkeys.size(); i++) {
        Bkey top = s.top();
        if (top.m_off + top.m_len < bkeys[i].m_off) {
            s.push(bkeys[i]);
        } else if ((top.m_off+top.m_len) < (bkeys[i].m_off + bkeys[i].m_len)) {
            top.m_len = bkeys[i].m_off+bkeys[i].m_len - top.m_off;
            s.pop();
            s.push(top);
        }
    }
    while (!s.empty()) {
        Bkey t = s.top();
        s.pop();
        merged_bkeys.push_back(t);
    }

    sort(merged_bkeys.begin(), merged_bkeys.end(), BkeyOffsetCompare);
}

void Bcache::_find_miss_region(off_t off, size_t len,
                               const vector<Bkey>& merged_bkeys,
                               vector<Bkey>& miss_bkeys) {
    off_t  cur = off;
    size_t left = len;

    vector<Bkey>::const_iterator p = merged_bkeys.begin();
    while (left > 0) {
        if (p == merged_bkeys.end()) {
            Bkey bkey(cur, left, IoVersion(0, 0));
            miss_bkeys.push_back(bkey);
            cur += left;
            break;
        }

        if (cur < p->m_off) {
            off_t  next = p->m_off;
            size_t len  = MIN(next-cur, left);
            Bkey key(cur, len, IoVersion(0, 0));
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
                            bcache_map_t& hit_cache_snapshot) {
    LOG_DEBUG << "hit read start ";
    for (int i = 0; i < hit_keys.size(); i++) {
        Bkey k = hit_keys[i];

        shared_ptr<CEntry> v;
        auto it = hit_cache_snapshot.find(k);
        if (it == hit_cache_snapshot.end()) {
            LOG_ERROR << "cache snapshot find failed";
            break;
        }
        v = it->second;
        char*  pdst = NULL;
        size_t pdst_len  = 0;
        off_t  pdst_off  = 0;
        if (off > k.m_off) {
            pdst_off = off;
            pdst_len = MIN(len, k.m_off+k.m_len - off);
        } else {
            pdst_off  = k.m_off;
            pdst_len  = MIN(k.m_len, off+len-k.m_off);
        }
        if (pdst_off < off) {
            LOG_ERROR << "pdst_off < off";
            continue;
        }
        pdst = buf + (pdst_off-off);
        shared_ptr<JournalEntry> journal_entry = nullptr;
        uint8_t cache_type = v->get_cache_type();
        if (cache_type == CEntry::IN_MEM) {
            //in memory
            //todo: one log header has many io
            LOG_DEBUG << "hit read from memory";
            journal_entry = v->get_journal_entry();
            LOG_DEBUG << "hit read from memory ok";
        } else if (cache_type == CEntry::IN_JOURANL) {
            //in log
            //todo: one log header has many io
            LOG_DEBUG << "hit read from journal";
            string journal_file = v->get_journal_file();
            off_t  journal_off  = v->get_journal_off();
            journal_entry = std::make_shared<JournalEntry>();
            unique_ptr<AccessFile> access_file;
            Env::instance()->create_access_file(journal_file, false, &access_file);
            size_t file_size = Env::instance()->file_size(journal_file);
            journal_entry->parse(&access_file, file_size, journal_off);
            LOG_DEBUG << "hit read from journal ok ";
        } else {
            //assert(0);
        }
        shared_ptr<Message> message = journal_entry->get_message();
        shared_ptr<WriteMessage> write_message = dynamic_pointer_cast
                                                  <WriteMessage>(message);
        char* psrc = (char*)write_message->data().c_str();
        psrc += pdst_off-k.m_off;
        memcpy(pdst, psrc, pdst_len);
    }

    LOG_DEBUG << "hit read ok ";
    return 0;
}

int Bcache::_cache_miss_read(off_t off, size_t len, char* buf,
                             const vector<Bkey>& miss_keys) {
    LOG_INFO << "miss read start ";
    for (auto it : miss_keys) {
        Bkey k = it;
        char*  pbuf = nullptr;
        size_t pbuf_len = 0;
        off_t  pbuf_off = 0;
        if (off > k.m_off) {
            if (off > k.m_off + k.m_len) {
                continue;
            } else {
                pbuf_off = off;
                pbuf_len = MIN(len, k.m_off+k.m_len - off);
            }
        } else if (k.m_off > off + len) {
            continue;
        } else {
            pbuf_off = k.m_off;
            pbuf_len = MIN(k.m_len, off+len-k.m_off);
        }
        pbuf = buf+(pbuf_off - off);
        m_blkfile->read(pbuf, pbuf_len, pbuf_off);
    }
    LOG_INFO << "miss read ok";
    return 0;
}

int Bcache::read(off_t off, size_t len, char* buf) {
    /*snapshot store which bkey and centry pair cache hit*/
    bcache_map_t region_hits;

    /*find cache hit  region, no seq warrant*/
    {
        ReadLock read(m_mutex);
        _find_hit_region(off, len, region_hits);
    }
    if (!region_hits.empty()) {
        LOG_DEBUG << "read region hit ";
        for (auto it : region_hits) {
            LOG_DEBUG << "\t" << it.first.m_off << "--" << it.first.m_len << "--" << it.first.m_seq;
        }

        /*cache hit region, make seq warrant */
        vector<Bkey> order_hit_keys;
        for (auto it : region_hits) {
            order_hit_keys.push_back(it.first);
        }
        sort(order_hit_keys.begin(), order_hit_keys.end(), BkeySeqCompare);

        LOG_DEBUG << "read region hit order hit keys" << endl;
        for (auto it : order_hit_keys) {
            LOG_DEBUG << "\t" << it.m_off << "--" << it.m_len << "--" << it.m_seq;
        }

        /*temporay cache hit merge for latter step*/
        vector<Bkey> merged_keys;
        _merge_hit_region(order_hit_keys, merged_keys);

        LOG_DEBUG << "read region merged keys" << endl;
        for (auto it : merged_keys) {
            LOG_DEBUG << "\t" << it.m_off << "******" << it.m_len;
        }

        /*find miss region*/
        vector<Bkey> order_miss_keys;
        _find_miss_region(off, len, merged_keys, order_miss_keys);

        LOG_DEBUG << "read region miss keys";
        for (auto it : order_miss_keys) {
            LOG_DEBUG << "\t" << it.m_off << "%%%%%%" << it.m_len;
        }

        /*read from cache*/
        if (!order_hit_keys.empty()) {
            _cache_hit_read(off, len, buf, order_hit_keys, region_hits);
        }

        /*read from device*/
        if (!order_miss_keys.empty()) {
            _cache_miss_read(off, len, buf, order_miss_keys);
        }

    } else {
        LOG_DEBUG << "read region no hit ";
        vector<Bkey> order_miss_keys;
        order_miss_keys.push_back(Bkey(off, len, IoVersion(0, 0)));
        _cache_miss_read(off, len, buf, order_miss_keys);
    }
    return 0;
}

void Bcache::trace() {
    for (auto it : m_bcache) {
        LOG_INFO << "\t" << "(" << it.first.m_off << " "  \
                 << it.first.m_len << " "  \
                 << it.first.m_seq << ")"  \
                 <<"[" << it.second.use_count() << "]";
    }
}
