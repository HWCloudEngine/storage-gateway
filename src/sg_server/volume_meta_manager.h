/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    volume_meta_manager.h
* Author: 
* Date:         2016/07/11
* Version:      1.0
* Description: interface for volume meta management
* 
************************************************/
#ifndef VOLUME_META_MANAGER_H_
#define VOLUME_META_MANAGER_H_
#include <list>
#include <string>
#include "rpc/common.pb.h"
#include "rpc/volume.pb.h"
using huawei::proto::VolumeMeta;
using huawei::proto::RESULT;
class VolumeMetaManager{
public:
    virtual ~VolumeMetaManager(){}
    virtual RESULT list_volume_meta(std::list<VolumeMeta> &list) = 0;
    virtual RESULT read_volume_meta(const std::string& vol_id,
                VolumeMeta& meta) = 0;
    virtual RESULT update_volume_meta(const VolumeMeta& meta) = 0;
    virtual RESULT create_volume(const VolumeMeta& meta) = 0;
    virtual RESULT delete_volume(const std::string& vol_id) = 0;
};
#endif
