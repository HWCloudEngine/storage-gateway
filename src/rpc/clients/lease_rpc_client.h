/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    lease_rpc_client.h
* Author: 
* Date:         2017/07/11
* Version:      1.0
* Description:
* 
************************************************/
#ifndef LEASE_RPC_CLIENT_H_
#define LEASE_RPC_CLIENT_H_
#include "rpc/lease.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <string>
using ::huawei::proto::UpdateLeaseReq;
using ::huawei::proto::UpdateLeaseAck;
using grpc::Channel;
using grpc::ClientContext;
using huawei::proto::Lease;

class LeaseRpcClient{
  public:
    explicit LeaseRpcClient() {
    }
    ~LeaseRpcClient(){}

    static StatusCode update_lease(const std::string& key,
                                   const std::string& value,
                                   std::map<std::string,std::string>& metas) {
        ClientContext context;
        UpdateLeaseAck ack;
        UpdateLeaseReq request;
        request.set_key(key);
        request.set_value(value);
        for(auto it = metas.begin(); it != metas.end(); ++it) {
            request.mutable_metas()->insert({it->first,it->second});
        }
        grpc::Status status = stub_->UpdateLease(&context, request, &ack);
        if(status.ok()) 
            return ack.status();
        else
            return StatusCode::sInternalError;
    }

    static void init(std::shared_ptr<Channel> channel){
        stub_.reset(new Lease::Stub(channel));
    }
    
  private:
    static std::unique_ptr<Lease::Stub> stub_;
};
#endif
