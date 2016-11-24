#ifndef _INDEX_STORE_H
#define _INDEX_STORE_H
#include <string>
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

    virtual int db_open()  = 0;
    virtual int db_close() = 0;

    virtual int    db_put(string key, string value) = 0;
    virtual string db_get(string key) = 0;
    virtual int    db_del(string key) = 0;
};

class RocksDbIndexStore : public IndexStore
{
public:
    RocksDbIndexStore(string db_path):m_db_path(db_path){
        db_open();
    }

    ~RocksDbIndexStore(){
        db_close();
    }

    int db_open() override;
    int db_close() override;

    int    db_put(string key, string value) override;
    string db_get(string key) override;
    int    db_del(string key) override;

private:
    string  m_db_path;
    Options m_db_option;
    DB*     m_db;
};

#endif
