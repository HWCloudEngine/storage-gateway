/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_meta_manager.h
* Author: 
* Date:         2016/07/11
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef JOURNAL_META_MANAGER_H_
#define JOURNAL_META_MANAGER_H_
#include <string>
#include <list>
#include "rpc/common.pb.h"
#include "rpc/journal.pb.h"
#include "rpc/consumer.pb.h"
#include "rpc/writer.pb.h"
using huawei::proto::JOURNAL_STATUS;
using huawei::proto::CONSUMER_TYPE;
using huawei::proto::JournalMarker;
using huawei::proto::RESULT;
using huawei::proto::JournalMeta;
using std::string;

class JournalMetaManager {
public:
    virtual ~JournalMetaManager() {}
    virtual RESULT create_journals(const string& uuid,const string& vol_id,
            const int& limit, std::list<string> &list) = 0;
    virtual RESULT seal_volume_journals(const string& uuid,const string& vol_id,
            const string journals[],const int& count) = 0;
    virtual RESULT get_journal_marker(const string& uuid,const string& vol_id,
            const CONSUMER_TYPE& type,JournalMarker* marker) = 0;
    virtual RESULT update_journals_marker(const string& uuid, const string& vol_id,
            const CONSUMER_TYPE& type,const JournalMarker& marker) = 0;
    virtual RESULT get_consumable_journals(const string& uuid,const string& vol_id,
            const JournalMarker& marker,const int& limit, std::list<string> &list) = 0;
};
#endif
