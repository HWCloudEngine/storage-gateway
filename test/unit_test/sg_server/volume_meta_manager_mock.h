/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    volume_meta_manager_mock.h
* Author: 
* Date:         2017/04/27
* Version:      1.0
* Description:
* 
************************************************/
#ifndef VOLUME_META_MANAGER_MOCK_H_
#define VOLUME_META_MANAGER_MOCK_H_

#include "sg_server/volume_meta_manager.h"
#include "gmock/gmock.h"
// tricky, in order to test private methods
#define private public

class MockVolumeMetaManager:public VolumeMetaManager{
public:
    MOCK_METHOD1(list_volume_meta, RESULT(std::list<VolumeMeta> &list));

    MOCK_METHOD2(read_volume_meta, RESULT (const std::string& vol_id,
                VolumeMeta& meta));

    MOCK_METHOD1(update_volume_meta, RESULT (const VolumeMeta& meta));

    MOCK_METHOD1(create_volume, RESULT (const VolumeMeta& meta));

    MOCK_METHOD1(delete_volume, RESULT (const std::string& vol_id));

};
#endif
