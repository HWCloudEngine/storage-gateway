/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    lease_rpc_service.h
* Author: 
* Date:         2017/07/11
* Version:      1.0
* Description:
* 
***********************************************/
#ifndef LEASE_RPC_SERVICE_H_
#define LEASE_RPC_SERVICE_H_
#include "rpc/lease.grpc.pb.h"
#include "common/kv_api.h"
using ::huawei::proto::UpdateLeaseReq;
using ::huawei::proto::UpdateLeaseAck;

class LeaseRpcSrv:public huawei::proto::Lease::Service {
  public:
    LeaseRpcSrv(std::shared_ptr<KVApi> kv_ptr);
    ~LeaseRpcSrv();
    ::grpc::Status UpdateLease(::grpc::ServerContext* context,
                               const UpdateLeaseReq* request,
                               UpdateLeaseAck* response) override;
  private:
    std::shared_ptr<KVApi> kv_ptr_;
};
#endif
