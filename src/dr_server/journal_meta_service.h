/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_meta_service.h
* Author: 
* Date:         2016/07/11
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef JOURNAL_META_SERVICE_H_
#define JOURNAL_META_SERVICE_H_
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

class JournalMetaService {
public:
    virtual ~JournalMetaService() {}
    virtual RESULT get_volume_journals(string vol_id,int limit, 
            std::list<string> &list) = 0;
    virtual RESULT delete_journals(string vol_id, string journals[], int count) = 0;
    virtual RESULT seal_volume_journals(string vol_id, string journals[],
            int count) = 0;
    virtual RESULT get_journal_marker(string vol_id, CONSUMER_TYPE type,
            JournalMarker* marker) = 0;
	virtual RESULT update_journals_marker(string vol_id, CONSUMER_TYPE type,
            JournalMarker marker) = 0;
    virtual RESULT get_consumer_journals(string vol_id, JournalMarker marker,
            int limit, std::list<string> &list) = 0;
};
#endif
