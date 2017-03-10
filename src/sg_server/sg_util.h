/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    sg_util.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef SG_UTIL_H_
#define SG_UTIL_H_
#include <string>
#include <cstdint>
#include "rpc/journal.pb.h"
#include "common/journal_meta_handle.h"
#include "ceph_s3_meta.h"
using std::string;
using google::protobuf::int64;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::RESULT;
using huawei::proto::JournalMarker;
namespace sg_util{
inline string counter_to_string(const uint64_t& counter) {
    char tmp[PRINT_COUNTER_BITS+1] = {0};
    std::sprintf(tmp,"%0*lX",PRINT_COUNTER_BITS,counter);
    string counter_s(tmp);
    return counter_s;
}

inline bool extract_counter_from_object_key(const string& key,uint64_t& cnt){
    std::size_t pos = key.find_last_of('/',string::npos);
    // assert(pos != string::npos);
    string counter_s = key.substr(pos+1,string::npos);
    try {
        cnt = std::stoull(counter_s,nullptr,16);
    }catch (const std::invalid_argument& ia) {
        LOG_ERROR << "Invalid argument: " << ia.what();
        return false;
    }
    return true;
}

inline bool get_path_by_journal_key(const string &key,string& path){
    JournalMeta meta;
    RESULT res = JournalMetaHandle::instance().get_journal_meta(key,meta);
    DR_ASSERT(res == DRS_OK);
    path = meta.path();
    return true;
}

inline string construct_journal_key(const string& vol_id,const int64& counter){
    string key = g_key_prefix;
    key += vol_id;
    key.append("/").append(counter_to_string(counter));
    return key;
}

inline string get_vol_by_key(const string &key){
    string temp = key.substr(0,key.find_last_of("/"));
    return temp.erase(0,temp.find_last_of("/")+1);
}

};
#endif
