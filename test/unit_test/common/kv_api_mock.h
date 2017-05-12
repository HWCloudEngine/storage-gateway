/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    kv_api_mock.h
* Author: 
* Date:         2017/05/12
* Version:      1.0
* Description:
* 
************************************************/
#ifndef KV_API_MOCK_H_
#define KV_API_MOCK_H_
#include "gmock/gmock.h"
#include "src/common/kv_api.h"

class MockKVApi:public KVApi{
public:
    MOCK_METHOD3(put_object, StatusCode(char, string, std::map));

    MOCK_METHOD1(delete_object,StatusCode(const char* key));

    MOCK_METHOD5(list_objects,
        StatusCode(char, char, int, std::list, char));

    MOCK_METHOD2(get_object,
        StatusCode(const char* key, string* value));

    MOCK_METHOD2(head_object,StatusCode(char, std::map));
};
#endif
