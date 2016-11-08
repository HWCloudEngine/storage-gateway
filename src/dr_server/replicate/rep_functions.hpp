/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_functions.hpp
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_FUNCTIONS_HPP_
#define REP_FUNCTIONS_HPP_
#include <string>
#include <cstdint>
#include "rpc/journal.pb.h"
#include "common/journal_meta_handle.hpp"
using std::string;
using google::protobuf::int64;

namespace replicate{

inline int64 get_counter(const string& key){
    return std::stoll(key.substr(key.find_last_of('/')+1),nullptr);
}

inline bool get_path_by_journal_key(const string &key,string& path){
    JournalMeta meta;
    RESULT res = JournalMetaHandle::instance().get_journal_meta(key,meta);
    DR_ASSERT(res == DRS_OK);
    path = meta.path();
    return true;
}

inline string construct_journal_key(const string& vol_id,const int64& counter){
    string key("/journals/");
    key += vol_id;
    char temp[13];
    sprintf(temp,"%012lld",counter);
    key.append("/").append(temp);
    return key;
}

inline string get_vol_by_key(const string &key){
    string temp = key.substr(0,key.find_last_of("/"));
    return temp.erase(0,temp.find_last_of("/")+1);
}
};
#endif
