/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    db_meta_api.cc
* Author: 
* Date:         2017/07/05
* Version:      1.0
* Description:  implementation of KVApi with index_store
* 
************************************************/
#include "db_meta_api.h"
#include <stdexcept>
#include <cstddef>
#include <set>
#include "define.h"
#include "log/log.h"

#define MAX_VALUE_LEN   8192
#define VALUE_LEN_BYTES 4
const char SEPARATOR='@'; // not rigorous, suppose that no @ contained in meta

DBMetaApi::DBMetaApi(IndexStore* index_store)
        :m_index_store(index_store) {
    SG_ASSERT(m_index_store != nullptr);
    SG_ASSERT(-1 != m_index_store->db_open());
}

DBMetaApi::~DBMetaApi(){
    if (m_index_store) {
        delete m_index_store;
    }
}

StatusCode DBMetaApi::put_object(const char* obj_name, const string* value,
            const std::map<string,string>* meta){
    string serialized_value;
    encode_meta_value(value,meta,&serialized_value);
    if(0 == m_index_store->db_put(string(obj_name),serialized_value)){
        return StatusCode::sOk;
    }
    else{
        return StatusCode::sInternalError;
    }
}

StatusCode DBMetaApi::delete_object(const char* key){
    if(0 == m_index_store->db_del(string(key))){
        return StatusCode::sOk;
    }
    else{
        LOG_ERROR << "delete meta key failed:" << key;
        return StatusCode::sInternalError;
    }
}

// suppose that dilimiter has only one char,mostly is '/'
StatusCode DBMetaApi::list_objects(const char*prefix, const char*marker,
        int maxkeys, std::list<string>* list, const char* delimiter){
    SG_ASSERT(prefix != nullptr);
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(string(prefix));
    int cnt = 0;
    LOG_DEBUG << "list objects,prefix:" << prefix;
    if(delimiter == nullptr){
        while(it->valid() && (maxkeys <= 0 || cnt <= maxkeys)){
            LOG_DEBUG << " " << it->key();
            if(marker != nullptr && it->key().compare(marker)<=0){
                it->next();
                continue;
            }
            list->push_back(it->key());
            it->next();
            cnt++;
        }
    }
    else{
        std::set<string> set;
        size_t off = strlen(prefix);
        while(it->valid() && (maxkeys <= 0 || cnt <= maxkeys)){
            size_t pos = it->key().find_first_of(delimiter,off);
            if(pos == string::npos){
                it->next();
                continue;
            }
            set.insert(it->key().substr(0,pos+1));
            it->next();
            cnt++;
        }
        LOG_DEBUG << "list with prefix:" << prefix << ",delimiter:" << delimiter;
        for (auto it2=set.begin(); it2!=set.end(); ++it2){
            list->push_back(*it2);
            LOG_DEBUG << " " << *it2;
        }
    }
    return StatusCode::sOk;
}

StatusCode DBMetaApi::get_object(const char* key, string* value){
    if(m_index_store->db_key_not_exist(key)) {
        LOG_DEBUG << "object not found:" << key;
        return StatusCode::sNotFound;
    }

    string raw_value = m_index_store->db_get(key);
    SG_ASSERT(0 == decode_meta_value(&raw_value,value,nullptr));

    return StatusCode::sOk;
}

StatusCode DBMetaApi::head_object(const char* key,
        std::map<string,string>* meta){
    if(m_index_store->db_key_not_exist(key)) {
        LOG_DEBUG << "object not found:" << key;
        return StatusCode::sNotFound;
    }

    string raw_value = m_index_store->db_get(key);
    string value;
    SG_ASSERT(0 == decode_meta_value(&raw_value,&value,meta));
    return StatusCode::sOk;
}

int DBMetaApi::decode_meta_value(const string* in,string* out,
        std::map<string,string>* meta){
    int len = 0;
    try{
        len = std::stoi(in->substr(0,VALUE_LEN_BYTES));
    }
    catch(const std::invalid_argument &e){
        LOG_ERROR << "invalid_argument len:" << in->substr(0,VALUE_LEN_BYTES);
        DR_ERROR_OCCURED();
    }

    if(len + VALUE_LEN_BYTES == in->length()){
        out->append(in->substr(VALUE_LEN_BYTES));
        return 0;
    }

    out->append(in->substr(VALUE_LEN_BYTES,len));
    if(meta != nullptr) {
        int off = len + VALUE_LEN_BYTES;
        int max = in->length();
        while(off <= max) {
            SG_ASSERT(in->at(off) == SEPARATOR);
            off++;
            size_t value_pos = in->find_first_of(SEPARATOR,off);
            SG_ASSERT(string::npos != value_pos);
            string key = in->substr(off,value_pos-off);
            off = value_pos+1;
            size_t next_key_pos = in->find_first_of(SEPARATOR,off);
            if(string::npos == next_key_pos) {
                string value = in->substr(off);
                meta->insert({key,value});
                LOG_DEBUG << "decode kv, key=" << key << ",value=" << value;
                break;
            }
            else {
                string value = in->substr(off,next_key_pos-off);
                meta->insert({key,value});
                LOG_DEBUG << "decode kv, key=" << key << ",value=" << value;
                off = next_key_pos;
            }
        }
    }

    return 0;
}

// encode:set the value length at the first 4 bytes;
// append the key:value meta data at the end of value string
int DBMetaApi::encode_meta_value(const string* in,
        const std::map<string,string>* in_meta,
        string* out){
    SG_ASSERT(in->length() <= MAX_VALUE_LEN);
    char buf[VALUE_LEN_BYTES+1];
    std::sprintf(buf,"%0*lu",VALUE_LEN_BYTES,in->length());
    out->append(buf,VALUE_LEN_BYTES);
    out->append(*in);
    if(in_meta != nullptr && !in_meta->empty()){
        for(auto it=in_meta->begin(); it!=in_meta->end(); it++){
            out->append(1,SEPARATOR);
            out->append(it->first);
            out->append(1,SEPARATOR);
            out->append(it->second);
        }
    }
    return 0;
}

