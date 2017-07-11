/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    db_meta_api.h
* Author: 
* Date:         2017/07/05
* Version:      1.0
* Description:
* 
************************************************/
#ifndef DB_META_API_H_
#define DB_META_API_H_
#include "kv_api.h"
#include "index_store.h"
class DBMetaApi: public KVApi{
public:
    DBMetaApi(IndexStore* index_store);
    virtual ~DBMetaApi();

    virtual StatusCode put_object(const char* obj_name, const string* value,
            const std::map<string,string>* meta=nullptr);

    virtual StatusCode delete_object(const char* key);

    virtual StatusCode list_objects(const char*prefix, const char*marker,
            int maxkeys, std::list<string>* list, const char* delimiter=nullptr);

    virtual StatusCode get_object(const char* key, string* value);

    virtual StatusCode head_object(const char* key,
            std::map<string,string>* meta=nullptr);

private:
    IndexStore* m_index_store;

    int decode_meta_value(const string* in,string* out,
        std::map<string,string>* meta);

    int encode_meta_value(const string* in,const std::map<string,string>* in_meta,
        string* out);
};
#endif

