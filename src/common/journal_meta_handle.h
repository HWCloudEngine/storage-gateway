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
#include "sg_server/ceph_s3_api.h"
#include "common/config_parser.h"
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
        std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
        if(false == parser->get<string>("ceph_s3.access_key",access_key)){
            LOG_FATAL << "config parse ceph_s3.access_key error!";
            DR_ERROR_OCCURED();
        }
        if(false == parser->get<string>("ceph_s3.secret_key",secret_key)){
            LOG_FATAL << "config parse ceph_s3.secret_key error!";
            DR_ERROR_OCCURED();
        }
        // port number is necessary if not using default 80/443
        if(false == parser->get<string>("ceph_s3.host",host)){
            LOG_FATAL << "config parse ceph_s3.host error!";
            DR_ERROR_OCCURED();
        }
        if(false == parser->get<string>("ceph_s3.bucket",bucket_name)){
            LOG_FATAL << "config parse ceph_s3.bucket error!";
            DR_ERROR_OCCURED();
        }
        s3Api_ptr_.reset(new CephS3Api(access_key.c_str(),
                secret_key.c_str(),host.c_str(),bucket_name.c_str()));        
    }
    ~JournalMetaHandle(){
        s3Api_ptr_.reset();
    }
    std::unique_ptr<CephS3Api> s3Api_ptr_;
};
#endif
