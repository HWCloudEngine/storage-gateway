#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <iostream>
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "../log/log.h"
#include "index_store.h"

IndexStore* IndexStore::create(const string& type, const string& db_path)
{
    if(type == "rocksdb"){
        return new RocksDbIndexStore(db_path); 
    }
    return nullptr;
}

int RocksDbIndexStore::db_open()
{
    m_db_option.IncreaseParallelism();
    m_db_option.OptimizeLevelStyleCompaction();
    m_db_option.create_if_missing = true;
    m_db_option.prefix_extractor.reset(NewFixedPrefixTransform(3));
    
    string file_lock = m_db_path + "/" + "LOCK";
    if(access(file_lock.c_str(), F_OK) == 0){
        /*file lock exist, will open db failed , so remove it*/ 
        int ret = unlink(file_lock.c_str());
        assert(ret == 0);
    }

    Status s = DB::Open(m_db_option, m_db_path, &m_db);
    if(!s.ok()){
        LOG_ERROR << "RocksDbIndexStore open failed:" << s.ToString(); 
        return -1;
    }

    LOG_INFO << "RocksDbIndexStore open"
             << " db_path:" << m_db_path
             << " status:"  << s.ToString();
    return 0;
}

int RocksDbIndexStore::db_close()
{
    if(m_db){
        delete m_db;
    }
    LOG_INFO << "RocksDbIndexStore close"
             << " db_path:" << m_db_path
             << " ok";
    return 0;
}

int RocksDbIndexStore::db_put(string key, string value)
{
    Status s = m_db->Put(WriteOptions(), key, value);
    assert(s.ok());

    LOG_INFO << "RocksDbIndexStore put"
             << " key:" << key
             << " val:" << value
             << " ok";
    return 0;
}

string RocksDbIndexStore::db_get(string key)
{
    string value;
    Status s = m_db->Get(ReadOptions(), key, &value);
    assert(s.ok());

    LOG_INFO << "RocksDbIndexStore get"
             << " key:" << key
             << " val:" << value
             << " ok";
    return value;
}

int RocksDbIndexStore::db_del(string key)
{
    Status s = m_db->Delete(WriteOptions(), key);
    assert(s.ok());

    LOG_INFO << "RocksDbIndexStore del"
             << " key:" << key
             << " ok";
    return 0;
}

int RocksDbIndexStore::IteratorImpl::seek_to_first()
{
    m_db_iter->SeekToFirst();
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::seek_to_last()
{
    m_db_iter->SeekToFirst();
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::seek_to_first(const string& prefix)
{
    rocksdb::Slice slice_prefix(prefix);
    m_db_iter->Seek(slice_prefix);
    return m_db_iter->status().ok() ? 0 : -1;
}

int RocksDbIndexStore::IteratorImpl::seek_to_last(const string& prefix)
{
    return 0;
}

bool RocksDbIndexStore::IteratorImpl::valid()
{
    return m_db_iter->Valid();
}

int RocksDbIndexStore::IteratorImpl::next()
{
    if(valid()){
        m_db_iter->Next(); 
    }
    return m_db_iter->status().ok() ? 0 : -1;
}

string RocksDbIndexStore::IteratorImpl::key()
{
    return m_db_iter->key().ToString(false);
}

string RocksDbIndexStore::IteratorImpl::value()
{
    return m_db_iter->value().ToString(false);
}

IndexStore::SimpleIteratorPtr RocksDbIndexStore::db_iterator()
{
    assert(m_db != nullptr);

    return make_shared<IteratorImpl>(m_db->NewIterator(rocksdb::ReadOptions()));
}

void RocksDbIndexStore::RocksTransactionImpl::put(const string& key, 
                                                  const string& val)  
{
   batch_.Put(rocksdb::Slice(key), rocksdb::Slice(val)); 
}

void RocksDbIndexStore::RocksTransactionImpl::del(const string& key) 
{
    batch_.Delete(key);
}

IndexStore::Transaction RocksDbIndexStore::fetch_transaction()
{
    return make_shared<RocksTransactionImpl>();
}

int RocksDbIndexStore::submit_transaction(Transaction t)
{
    RocksTransactionImpl* trc = reinterpret_cast<RocksTransactionImpl*>(t.get());
    rocksdb::WriteOptions wop;
    rocksdb::Status s = m_db->Write(wop, &(trc->batch_));
    return s.ok()? 0 : -1;
}



