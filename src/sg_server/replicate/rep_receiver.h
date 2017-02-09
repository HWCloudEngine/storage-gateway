/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_receiver.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_RECEIVER_H_
#define REP_RECEIVER_H_
#include <map>
#include <fstream>
#include <string>
#include <memory>
#include <thread>
#include "rpc/transfer.grpc.pb.h"
#include "../ceph_s3_meta.h"
#include "rep_type.h"
using std::string;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using huawei::proto::transfer::DataTransfer;
using grpc::ServerContext;
using grpc::ServerReaderWriter;

typedef struct Jkey{
    string vol_;
    int64 c_;
    Jkey(const string& vol,const int64& c):vol_(vol),c_(c){}
    bool operator<(const Jkey& j2)const{
        if(c_ != j2.c_)
            return c_ < j2.c_;
        return vol_.compare(j2.vol_);
    }
}Jkey;

class RepReceiver:public DataTransfer::Service{
private:
    std::string mount_path_;
    std::string uuid_;
    std::shared_ptr<CephS3Meta> meta_;
    std::map<std::string,JournalMarker> markers_;
public:
    RepReceiver(std::shared_ptr<CephS3Meta> meta,const std::string& path);
    grpc::Status transfer(ServerContext* context,
            ServerReaderWriter<TransferResponse,
            TransferRequest>* stream);
    grpc::Status sync_marker(ServerContext* context,
            const TransferRequest* req,
            TransferResponse* res);
private:
    bool write(const TransferRequest& req,
            std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map);
    bool init_journals(const TransferRequest& req,
            std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map);
    bool seal_journals(const TransferRequest& req,
            std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map);
    std::shared_ptr<std::ofstream> get_fstream(const string& vol,
            const int64& counter,
            const std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map);
};

#endif

