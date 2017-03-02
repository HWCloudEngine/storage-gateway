/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_meta_handle.hpp
* Author: 
* Date:         2016/10/25
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef JOURNAL_META_HANDLE_H_
#define JOURNAL_META_HANDLE_H_
#include "rpc/common.pb.h"
#include "rpc/journal.pb.h"
#include "common/ceph_s3_api.h"
#include "common/config.h"
#include "log/log.h"
#include <string>
using huawei::proto::JournalMeta;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

class JournalMetaHandle{
public:
    RESULT get_journal_meta(const std::string& key,
            JournalMeta &meta){
        string value;
        RESULT res = s3Api_ptr_->get_object(key.c_str(),&value);
        if(res != DRS_OK)
            return res;
        if(true != meta.ParseFromString(value)){
            LOG_ERROR << "parser journal " << key <<" 's meta failed!";
            return INTERNAL_ERROR;
        }
        return DRS_OK;
    }
    static JournalMetaHandle& instance(){
        static JournalMetaHandle instance;
        return instance;
    }
    JournalMetaHandle(JournalMetaHandle&) = delete;
    JournalMetaHandle& operator=(JournalMetaHandle const&) = delete;
private:
    JournalMetaHandle(){
        string access_key;
        string secret_key;
        string host;
        string bucket_name;
        
        Configure conf;
        conf.init(DEFAULT_CONFIG_FILE);
        access_key = conf.ceph_s3_access_key;
        secret_key = conf.ceph_s3_secret_key;
        host = conf.ceph_s3_host;
        bucket_name = conf.ceph_s3_bucket;

        s3Api_ptr_.reset(new CephS3Api(access_key.c_str(),
                    secret_key.c_str(),host.c_str(),bucket_name.c_str()));        
    }
    ~JournalMetaHandle(){
        s3Api_ptr_.reset();
    }
    std::unique_ptr<CephS3Api> s3Api_ptr_;
};
#endif
