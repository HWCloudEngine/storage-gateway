/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   index store for snapshot and backup meta
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: meta data store for snapshot and backup
*
*************************************************/
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <iostream>
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "log/log.h"
#include "index_store.h"

IndexStore* IndexStore::create(const std::string& type, const std::string& db_path) {
    if (type == "rocksdb") {
        return new RocksDbIndexStore(db_path);
    }
    return nullptr;
}

int RocksDbIndexStore::db_open() {
    m_db_option.IncreaseParallelism();
    m_db_option.OptimizeLevelStyleCompaction();
    m_db_option.create_if_missing = true;
    m_db_option.prefix_extractor.reset(NewFixedPrefixTransform(3));
    std::string file_lock = m_db_path + "/" + "LOCK";
    if (access(file_lock.c_str(), F_OK) == 0) {
        /*file lock exist, will open db failed , so remove it*/
        int ret = unlink(file_lock.c_str());
        assert(ret == 0);
    }
    Status s = DB::Open(m_db_option, m_db_path, &m_db);
    if (!s.ok()) {
        LOG_ERROR << "open db:" << m_db_path << " failed:" << s.ToString();
        return -1;
    }
    return 0;
}

int RocksDbIndexStore::db_close() {
    if (m_db) {
        delete m_db;
    }
    return 0;
}

int RocksDbIndexStore::db_put(const std::string& key, const std::string& value) {
    Status s = m_db->Put(WriteOptions(), key, value);
    assert(s.ok());
    return 0;
}

std::string RocksDbIndexStore::db_get(const std::string& key) {
    std::string value;
    Status s = m_db->Get(ReadOptions(), key, &value);
    assert(s.ok());
    return value;
}

int RocksDbIndexStore::db_del(const std::string& key) {
    Status s = m_db->Delete(WriteOptions(), key);
    assert(s.ok());
    return 0;
}

int RocksDbIndexStore::IteratorImpl::seek_to_first() {
    m_db_iter->SeekToFirst();
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::seek_to_last() {
    m_db_iter->SeekToLast();
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::seek_to_first(const std::string& prefix) {
    rocksdb::Slice slice_prefix(prefix);
    m_db_iter->Seek(slice_prefix);
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::seek_to_last(const std::string& prefix) {
    std::string limit = prefix;
    limit.push_back(1);
    rocksdb::Slice slice_limit(limit);
    m_db_iter->Seek(slice_limit);
    if (m_db_iter->Valid()) {
        m_db_iter->SeekToLast();
        while (m_db_iter->Valid()) {
            if (m_db_iter->key().ToString(false).find(prefix) != -1) {
                break; 
            }
            m_db_iter->Prev();
        }
    }
    return m_db_iter->status().ok() ? 0 : -1;
}

bool RocksDbIndexStore::IteratorImpl::valid() {
    return m_db_iter->Valid();
}

int RocksDbIndexStore::IteratorImpl::prev() {
    if (valid()) {
        m_db_iter->Prev();
    }
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::next() {
    if (valid()) {
        m_db_iter->Next();
    }
    return m_db_iter->status().ok() ? 0 : -1;
}

std::string RocksDbIndexStore::IteratorImpl::key() {
    return m_db_iter->key().ToString(false);
}

std::string RocksDbIndexStore::IteratorImpl::value() {
    return m_db_iter->value().ToString(false);
}

IndexStore::IndexIterator RocksDbIndexStore::db_iterator() {
    assert(m_db != nullptr);
    return std::make_shared<IteratorImpl>(m_db->NewIterator(rocksdb::ReadOptions()));
}

void RocksDbIndexStore::RocksTransactionImpl::put(const std::string& key,
                                                  const std::string& val) {
   batch_.Put(rocksdb::Slice(key), rocksdb::Slice(val)); 
}

void RocksDbIndexStore::RocksTransactionImpl::del(const std::string& key) {
    batch_.Delete(key);
}

IndexStore::Transaction RocksDbIndexStore::fetch_transaction() {
    return std::make_shared<RocksTransactionImpl>();
}

int RocksDbIndexStore::submit_transaction(Transaction t) {
    RocksTransactionImpl* trc = reinterpret_cast<RocksTransactionImpl*>(t.get());
    rocksdb::WriteOptions wop;
    rocksdb::Status s = m_db->Write(wop, &(trc->batch_));
    return s.ok()? 0 : -1;
}
