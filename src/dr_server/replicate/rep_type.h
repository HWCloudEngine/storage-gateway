/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_type.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_TYPE_
#define REP_TYPE_
#include <cstdint>
#include <string>
typedef enum TASK_STATUS{
    T_UNKNOWN,
    T_WAITING,
    T_RUNNING,
    T_DONE,
    T_ERROR
}TASK_STATUS;

typedef struct JournalInfo{
    std::string key;
    std::string path;
    bool is_opened;
    uint64_t pos;
    uint64_t end;
}JournalInfo;
#endif