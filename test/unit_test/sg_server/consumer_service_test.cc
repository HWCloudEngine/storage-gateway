/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    consumer_server_test.cc
* Author: 
* Date:         2017/04/27
* Version:      1.0
* Description:
* 
************************************************/
#include "journal_meta_manager_mock.h"
#include "sg_server/consumer_service.h"
#include "gtest/gtest.h"

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgReferee;
using ::huawei::proto::DRS_OK;
using ::huawei::proto::INTERNAL_ERROR;
using ::huawei::proto::StatusCode;

class ConsumerServiceImplTest : public testing::Test{
 protected:
    ConsumerServiceImplTest():
        j_meta_mgr_mock(new MockJournalMetaManager()),
        consumer_service_impl(j_meta_mgr_mock) {
    }

    ~ConsumerServiceImplTest() {}

    std::shared_ptr<JournalMetaManager> j_meta_mgr_mock;
    ConsumerServiceImpl consumer_service_impl;
};

// user defined mather
MATCHER_P(IsMarkerEqual, m1, "") {
    if (arg.pos() != m1.pos()) {
        *result_listener << "offset is not equal!";
        return false;
    }
    if (arg.cur_journal().compare(m1.cur_journal()) != 0) {
        return false;
    }
    return true;
}

TEST_F(ConsumerServiceImplTest, GetJournalMarkerTest) {
    std::string volume("test_volume");
    CONSUMER_TYPE type = CONSUMER_TYPE::REPLAYER;
    JournalMarker marker;
    ServerContext context;
    GetJournalMarkerRequest request;
    GetJournalMarkerResponse reply;

    marker.set_cur_journal("/journals/test_volume/00000000000000000000");
    marker.set_pos(1024);
    auto mock_ptr =
        std::dynamic_pointer_cast<MockJournalMetaManager>(j_meta_mgr_mock);
    EXPECT_CALL(*mock_ptr, get_consumer_marker(volume, type, _))
        .Times(1)
        .WillOnce(DoAll(SetArgReferee<2>(marker), Return(DRS_OK)));

    request.set_vol_id(volume);
    request.set_type(type);
    grpc::Status ret =
        consumer_service_impl.GetJournalMarker(&context, &request, &reply);
    EXPECT_EQ(grpc::StatusCode::OK, ret.error_code());
//    marker.set_pos(2048);
    EXPECT_THAT(marker, IsMarkerEqual(reply.marker()));
}

