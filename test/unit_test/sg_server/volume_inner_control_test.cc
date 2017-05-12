/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    volume_inner_control_test.cc
* Author: 
* Date:         2017/04/27
* Version:      1.0
* Description:
* 
************************************************/
#include "volume_meta_manager_mock.h"
#include "journal_meta_manager_mock.h"
#include "gtest/gtest.h"
#include "sg_server/volume_inner_control.h"

using ::testing::Return;
using ::testing::_;
using ::huawei::proto::DRS_OK;
using ::huawei::proto::INTERNAL_ERROR;
using ::huawei::proto::StatusCode;

class VolumeInnerCtrlTest : public testing::Test{
protected:
    VolumeInnerCtrlTest():
        vol_meta_mgr(new MockVolumeMetaManager()),
        j_meta_mgr(new MockJournalMetaManager()){
    }

    ~VolumeInnerCtrlTest(){}

    std::shared_ptr<VolumeMetaManager> vol_meta_mgr;
    std::shared_ptr<JournalMetaManager> j_meta_mgr;
    VolInnerCtrl vol_inner_ctrl{vol_meta_mgr,j_meta_mgr};
};

TEST_F(VolumeInnerCtrlTest,CreateVolumeTest){
    std::string volume("test_volume");
    std::string path("/dev/xvde");
    uint64_t size = 16LU << 20;
    ServerContext context;
    CreateVolumeReq request;
    CreateVolumeRes response;
    auto vol_meta_mgr_mock =
        std::dynamic_pointer_cast<MockVolumeMetaManager>(vol_meta_mgr);
    EXPECT_CALL(*vol_meta_mgr_mock,read_volume_meta(volume,_))
        .Times(1)
        .WillOnce(Return (INTERNAL_ERROR));
    EXPECT_CALL(*vol_meta_mgr_mock,create_volume(_))
        .Times(1)
        .WillOnce(Return (DRS_OK));

    request.set_path(path);
    request.set_vol_id(volume);
    request.set_size(size);
    ::grpc::Status status =
        vol_inner_ctrl.CreateVolume(&context,&request,&response);
    EXPECT_EQ(status.error_code(),grpc::StatusCode::OK);
    EXPECT_EQ(StatusCode::sOk,response.status());
}

