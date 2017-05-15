/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    kv_api.h
* Author: 
* Date:         2017/05/12
* Version:      1.0
* Description:
* 
************************************************/
#ifndef KV_API_H_
#define KV_API_H_
#include "rpc/common.pb.h"
#include <string>
#include <list>
#include <map>
using huawei::proto::StatusCode;
using std::string;
class KVApi {
public:
    KVApi(){};
    virtual ~KVApi(){};

    virtual StatusCode put_object(const char* obj_name, const string* value,
            const std::map<string,string>* meta=nullptr) = 0;

    virtual StatusCode delete_object(const char* key) = 0;

    virtual StatusCode list_objects(const char*prefix, const char*marker,
            int maxkeys, std::list<string>* list, const char* delimiter=nullptr) = 0;

    virtual StatusCode get_object(const char* key, string* value) = 0;

    virtual StatusCode head_object(const char* key,
            std::map<string,string>* meta=nullptr){}
};
#endif
