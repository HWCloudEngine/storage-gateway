/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_message_handlers.h
* Author: 
* Date:         2017/03/02
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_MESSAGE_HANDLERS_H_
#define REP_MESSAGE_HANDLERS_H_
#include <fstream>
#include <string>
#include <memory>
#include <map>
#include <mutex>
#include <cstdint>
#include "sg_server/ceph_s3_meta.h"
#include "rpc/transfer.grpc.pb.h"
#include "common/ceph_s3_lease.h"
using std::string;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using huawei::proto::StatusCode;

typedef struct Jkey{
    string vol_;
    int64_t c_;
    Jkey(const string& vol,const int64_t& c):vol_(vol),c_(c){}
    bool operator<(const Jkey& j2)const{
        if(c_ != j2.c_)
            return c_ < j2.c_;
        return vol_.compare(j2.vol_);
    }
}Jkey;

class RepMsgHandlers{
private:
    std::string mount_path_;
    std::shared_ptr<CephS3Meta> meta_;
    std::mutex mutex_;
    std::map<const Jkey,std::shared_ptr<std::ofstream>> js_map_;
    std::shared_ptr<CephS3LeaseClient> lease_client_;
public:
    RepMsgHandlers(std::shared_ptr<CephS3Meta> meta,const std::string& path);
    ~RepMsgHandlers();
    StatusCode rep_handler(const TransferRequest& req);
private:
    // replicate related handle methods
    bool hanlde_replicate_data_req(const TransferRequest& req);
    bool handle_replicate_start_req(const TransferRequest& req);
    bool handle_replicate_end_req(const TransferRequest& req);
    bool handle_replicate_marker_req(const TransferRequest& req);
    std::shared_ptr<std::ofstream>  create_journal(
            const string& vol_id,const int64_t& counter);
    std::shared_ptr<std::ofstream> get_fstream(const string& vol,
            const int64_t& counter);
};
#endif
