/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_gc_manager.h
* Author: 
* Date:         2016/09/27
* Version:      1.0
* Description:  define gc manager interface
* 
***********************************************/
#ifndef JOURNAL_GC_MANAGER_H_
#define JOURNAL_GC_MANAGER_H_
#include <string>
#include <list>
#include "rpc/consumer.pb.h"
using huawei::proto::RESULT;
using huawei::proto::CONSUMER_TYPE;
using std::string;
class JournalGCManager{
public:
    virtual ~JournalGCManager() {} 
    virtual RESULT get_sealed_and_consumed_journals(
            const string& vol_id, const CONSUMER_TYPE& type,const int& limit,
            std::list<string> &list) = 0;
    virtual RESULT recycle_journals(const string& vol_id,
            const std::list<string>& journals) = 0;
    virtual RESULT get_producer_id(const string& vol_id,
            std::list<string>& list) = 0;
    virtual RESULT seal_opened_journals(const string& vol_id,
            const string& uuid) = 0;
    virtual RESULT list_volumes(std::list<string>& list) = 0;
};
#endif
