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
#ifndef SRC_COMMON_INDEX_STORE_H_
#define SRC_COMMON_INDEX_STORE_H_
#include <string>
#include <memory>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace rocksdb;

/*store snapshot meta data*/
class IndexStore {
 public:
    IndexStore(){}
    virtual ~IndexStore(){}
    static IndexStore* create(const std::string& type, const std::string& db_path);
    virtual int db_open()  = 0;
    virtual int db_close() = 0;
    virtual int db_put(const std::string& key, const std::string& value) = 0;
    virtual std::string db_get(const std::string& key) = 0;
    virtual int db_del(const std::string& key) = 0;

    /*iterator interface*/
    class IteratorInf {
     public:
        IteratorInf(){}
        virtual ~IteratorInf(){}
        virtual int seek_to_first() = 0;
        virtual int seek_to_last() = 0;
        virtual int seek_to_first(const std::string& prefix) = 0;
        virtual int seek_to_last(const std::string& prefix) = 0;
        virtual bool valid() = 0;
        virtual int prev() = 0;
        virtual int next() = 0;
        virtual std::string key() = 0;
        virtual std::string value() = 0;
    };
    typedef std::shared_ptr<IteratorInf> IndexIterator;
    virtual IndexIterator db_iterator()= 0;

    /*transaction interface*/
    class TransactionInf {
     public:
        virtual void put(const std::string& key, const std::string& val) = 0;
        virtual void del(const std::string& key) = 0;
    };
    typedef std::shared_ptr<TransactionInf> Transaction;
    virtual Transaction fetch_transaction() = 0;
    virtual int submit_transaction(Transaction t) = 0;
};

class RocksDbIndexStore : public IndexStore {

 public:
    explicit RocksDbIndexStore(const std::string& db_path) : m_db_path(db_path) {
        m_db = nullptr;
    }
    ~RocksDbIndexStore() {
        db_close();
    }
    
    int db_open() override;
    int db_close() override;
    int db_put(const std::string& key, const std::string& value) override;
    std::string db_get(const std::string& key) override;
    int db_del(const std::string& key) override;

    class IteratorImpl : public IndexStore::IteratorInf {
     public:
        explicit IteratorImpl(rocksdb::Iterator* db_iter) {
            m_db_iter = db_iter;
        }
        ~IteratorImpl() {
            if (m_db_iter) {
                delete m_db_iter;
            }
        }
        virtual int seek_to_first() override;
        virtual int seek_to_last() override;
        virtual int seek_to_first(const std::string& prefix) override;
        virtual int seek_to_last(const std::string& prefix) override;
        virtual bool valid() override;
        virtual int prev() override;
        virtual int next() override;
        virtual std::string key() override;
        virtual std::string value() override;
     private:
        rocksdb::Iterator* m_db_iter;
    };
    virtual IndexIterator db_iterator() override;

    class RocksTransactionImpl : public IndexStore::TransactionInf {
     public:
        virtual void put(const std::string& key, const std::string& val) override;
        virtual void del(const std::string& key) override;
        rocksdb::WriteBatch batch_;
    };
    virtual Transaction fetch_transaction() override;
    virtual int submit_transaction(Transaction t) override;

 private:
    std::string m_db_path;
    rocksdb::Options m_db_option;
    rocksdb::DB* m_db;
};
#endif  //  SRC_COMMON_INDEX_STORE_H_
