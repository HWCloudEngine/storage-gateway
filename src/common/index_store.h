#ifndef _INDEX_STORE_H
#define _INDEX_STORE_H
#include <string>
#include <memory>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace std;
using namespace rocksdb;

/*store snapshot meta data*/
class IndexStore
{
public:
    IndexStore(){}
    virtual ~IndexStore(){}

    static IndexStore* create(const string& type, const string& db_path);

    virtual int db_open()  = 0;
    virtual int db_close() = 0;

    virtual int    db_put(string key, string value) = 0;
    virtual string db_get(string key) = 0;
    virtual int    db_del(string key) = 0;
   
    /*iterator*/
    class SimpleIterator 
    {
    public:
        SimpleIterator(){}
        virtual ~SimpleIterator(){}

        virtual int seek_to_first() = 0;
        virtual int seek_to_last()  = 0;
        virtual int seek_to_first(const string& prefix) = 0;
        virtual int seek_to_last(const string& prefix) = 0;
        virtual bool valid()   = 0;
        virtual int  next()    = 0;
        virtual string key()   = 0;
        virtual string value() = 0;
    };
    typedef shared_ptr<SimpleIterator> SimpleIteratorPtr;    
    virtual SimpleIteratorPtr db_iterator()= 0;

    /*transaction*/
    class TransactionImpl
    {
    public:
        virtual void put(const string& key, const string& val) = 0;
        virtual void del(const string& key) = 0;
    };
    typedef shared_ptr<TransactionImpl> Transaction;

    virtual  Transaction fetch_transaction() = 0;
    virtual  int submit_transaction(Transaction t) = 0;
};

class RocksDbIndexStore : public IndexStore
{
public:
    explicit RocksDbIndexStore(const string& db_path):m_db_path(db_path){
        m_db = nullptr;
    }

    ~RocksDbIndexStore(){
        db_close();
    }
    
    int db_open() override;
    int db_close() override;

    int    db_put(string key, string value) override;
    string db_get(string key) override;
    int    db_del(string key) override;
    
    class IteratorImpl : public IndexStore::SimpleIterator
    {
    public:
        explicit IteratorImpl(rocksdb::Iterator* db_iter):m_db_iter(db_iter){
        }

        ~IteratorImpl(){
            if(m_db_iter){
                delete m_db_iter;    
            }
        }
        virtual int seek_to_first() override;
        virtual int seek_to_last()  override;
        virtual int seek_to_first(const string& prefix) override;
        virtual int seek_to_last(const string& prefix)  override;
        virtual bool valid()   override;
        virtual int  next()    override;
        virtual string key()   override;
        virtual string value() override;

    private:
        rocksdb::Iterator* m_db_iter;
    };
     
    virtual SimpleIteratorPtr db_iterator() override;

    class RocksTransactionImpl : public IndexStore::TransactionImpl
    {
    public:
        virtual void put(const string& key, const string& val) override;
        virtual void del(const string& key) override;
        rocksdb::WriteBatch batch_;
    };

    virtual  Transaction fetch_transaction() override;
    virtual  int submit_transaction(Transaction t) override;

private:
    string  m_db_path;
    rocksdb::Options m_db_option;
    rocksdb::DB* m_db;
};

#endif
