/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_meta_manager_mock.h
* Author: 
* Date:         2017/04/27
* Version:      1.0
* Description:
* 
************************************************/
#ifndef JOURNAL_META_MANAGER_MOCK_H_
#define JOURNAL_META_MANAGER_MOCK_H_
#include "gmock/gmock.h"
#include "sg_server/journal_meta_manager.h"

class MockJournalMetaManager:public JournalMetaManager {
public:

    MOCK_METHOD4(create_journals, RESULT (const string& uuid,
            const string& vol_id, const int& limit,
            std::list<JournalElement> &list));

    MOCK_METHOD3(create_journals_by_given_keys, RESULT (const string& uuid,
            const string& vol_id, const std::list<string> &list));

    MOCK_METHOD4(seal_volume_journals, RESULT (const string& uuid,
            const string& vol_id,
            const string journals[], const int& count));

    MOCK_METHOD3(get_consumer_marker,RESULT (const string& vol_id,
            const CONSUMER_TYPE& type, JournalMarker& marker));

    MOCK_METHOD3(update_consumer_marker, RESULT (const string& vol_id,
            const CONSUMER_TYPE& type,
            const JournalMarker& marker));

    MOCK_METHOD5(get_consumable_journals,RESULT (const string& vol_id,
            const JournalMarker& marker, const int& limit,
            std::list<JournalElement> &list,
            const CONSUMER_TYPE& type));

    MOCK_METHOD2(set_producer_marker, RESULT (const string& vol_id,
            const JournalMarker& marker));

    MOCK_METHOD2(get_journal_meta, RESULT (const string& key,
            huawei::proto::JournalMeta& meta));

    MOCK_METHOD3(get_producer_marker, RESULT (const string& vol_id,
            const CONSUMER_TYPE& type, JournalMarker& marker));

    MOCK_METHOD2(compare_journal_key, int (const string& key1,
            const string& key2));

    MOCK_METHOD2(compare_marker, int (const JournalMarker& m1,
            const JournalMarker& m2));

};
#endif
