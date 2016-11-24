#include <assert.h>
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "../log/log.h"
#include "index_store.h"

int RocksDbIndexStore::db_open()
{
    m_db_option.IncreaseParallelism();
    m_db_option.OptimizeLevelStyleCompaction();
    m_db_option.create_if_missing = true;
    m_db_option.prefix_extractor.reset(NewFixedPrefixTransform(3));
    Status s = DB::Open(m_db_option, m_db_path, &m_db);

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
