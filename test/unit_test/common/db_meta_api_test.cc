/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    db_meta_api_test.cc
* Author: 
* Date:         2017/07/05
* Version:      1.0
* Description:  test DBMetaApi
* 
************************************************/
#include "gtest/gtest.h"
#include "../common/index_store_mock.h"
#include "common/db_meta_api.h"

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgReferee;
using ::testing::AtLeast;
using ::testing::SetArgReferee;
using ::testing::Invoke;
class DBMetaApiTest : public testing::Test{
 protected:
    DBMetaApiTest():
        index_store(new MockIndexStore()),
        db_meta_api(new DBMetaApi(index_store)) {
    }

    ~DBMetaApiTest() {
        delete db_meta_api;
    }

    virtual void SetUp(){
        MockIndexStore* mock = (MockIndexStore*)index_store;
        EXPECT_CALL(*mock,db_put(_,_))
            .Times(AtLeast(7))
            .WillRepeatedly(DoAll(Invoke(this,&DBMetaApiTest::add_kv),Return(0)));

        string empty_vlume("");
        string value1("/journals/test_volume/00000000000000000000");
        db_meta_api->put_object(
                value1.c_str(),
                &value1,
                nullptr);
        string value2("/journals/test_volume/00000000000000010000");
        db_meta_api->put_object(
                value2.c_str(),
                &value2,
                nullptr);
        string value3("/journals/test_volume/00000000000000020000");
        db_meta_api->put_object(
                value3.c_str(),
                &value3,
                nullptr);

        string value4("/journals/test_volume/00000000000000010000:1024");
        db_meta_api->put_object(
                "/markers/test_volume/consumer/replayer",
                &value4,
                nullptr);
        db_meta_api->put_object(
                "/session/writer/test_volume/704d3a17-edcabe814261/00000000000000000000",
                &empty_vlume,
                nullptr);
        db_meta_api->put_object(
                "/session/writer/test_volume/704d3a17-edcabe814261/00000000000000010000",
                &empty_vlume,
                nullptr);
        db_meta_api->put_object(
                "/session/writer/test_volume/704d3a17-edcabe834261/00000000000000020000",
                &empty_vlume,
                nullptr);
    }

    void add_kv(const std::string& key, const std::string& value){
        kv_map.insert({key,value});
    }

    IndexStore* index_store;
    DBMetaApi* db_meta_api;
    std::map<string,string> kv_map;
};

TEST_F(DBMetaApiTest,get_object_test) {
    string value;
    MockIndexStore* mock = (MockIndexStore*)index_store;

    EXPECT_CALL(*mock,
                db_key_not_exist(_))
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    // not found
    EXPECT_EQ(StatusCode::sNotFound,
              db_meta_api->get_object("/journals/test_volume/00000000000000030000",
                                      &value));
    // get an existing key
    string value4("/journals/test_volume/00000000000000010000:1024");
    string key4("/markers/test_volume/consumer/replayer");
    EXPECT_CALL(*mock,db_get(key4))
        .Times(1)
        .WillOnce(Return(kv_map[key4]));
    value.clear();
    EXPECT_EQ(StatusCode::sOk,
              db_meta_api->get_object(key4.c_str(),
                                      &value));
    EXPECT_EQ(value,value4);
}

TEST_F(DBMetaApiTest,list_object_test)
{
    std::shared_ptr<MockIndexStore::MockIteratorInf> 
        it_ptr(new MockIndexStore::MockIteratorInf);
    MockIndexStore* mock = (MockIndexStore*)index_store;
    EXPECT_CALL(*mock,db_iterator( ))
        .Times(1)
        .WillOnce(Return(it_ptr));

    string prefix("/journals/test_volume/");
    string marker("/journals/test_volume/00000000000000010000");
    EXPECT_CALL(*it_ptr,seek_to_first(prefix))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*it_ptr,valid())
        .Times(4)
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*it_ptr,next())
        .Times(3)
        .WillRepeatedly(Return(0));
    string key0("/journals/test_volume/00000000000000000000");
    string key1("/journals/test_volume/00000000000000010000");
    string key2("/journals/test_volume/00000000000000020000");
    EXPECT_CALL(*it_ptr,key())
        .Times(7)
        .WillOnce(Return(key0))
        .WillOnce(Return(key0))
        .WillOnce(Return(key1))
        .WillOnce(Return(key1))
        .WillOnce(Return(key2))
        .WillOnce(Return(key2))
        .WillOnce(Return(key2));

    std::list<string> list;
    EXPECT_EQ(StatusCode::sOk,
              db_meta_api->list_objects(prefix.c_str(),
                                        marker.c_str(),
                                        10,
                                        &list,
                                        nullptr));
    EXPECT_EQ(1,list.size());
    EXPECT_EQ(key2,list.front());
}

TEST_F(DBMetaApiTest,list_object_test_with_delimiter) {
    std::shared_ptr<MockIndexStore::MockIteratorInf> 
        it_ptr(new MockIndexStore::MockIteratorInf);
    MockIndexStore* mock = (MockIndexStore*)index_store;
    EXPECT_CALL(*mock,db_iterator( ))
        .Times(1)
        .WillOnce(Return(it_ptr));

    string prefix("/session/writer/test_volume/");
    EXPECT_CALL(*it_ptr,seek_to_first(prefix))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*it_ptr,valid())
        .Times(4)
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*it_ptr,next())
        .Times(3)
        .WillRepeatedly(Return(0));
    string key0("/session/writer/test_volume/704d3a17-edcabe814261/00000000000000000000");
    string key1("/session/writer/test_volume/704d3a17-edcabe814261/00000000000000010000");
    string key2("/session/writer/test_volume/704d3a17-edcabe834261/00000000000000020000");
    EXPECT_CALL(*it_ptr,key())
        .Times(6)
        .WillOnce(Return(key0))
        .WillOnce(Return(key0))
        .WillOnce(Return(key1))
        .WillOnce(Return(key1))
        .WillOnce(Return(key2))
        .WillOnce(Return(key2));

    std::list<string> list;
    EXPECT_EQ(StatusCode::sOk,
              db_meta_api->list_objects(prefix.c_str(),
                                        nullptr,
                                        10,
                                        &list,
                                        "/"));
    EXPECT_EQ(2,list.size());
    EXPECT_EQ(string("/session/writer/test_volume/704d3a17-edcabe814261/"),list.front());
    list.pop_front();
    EXPECT_EQ(string("/session/writer/test_volume/704d3a17-edcabe834261/"),list.front());
}