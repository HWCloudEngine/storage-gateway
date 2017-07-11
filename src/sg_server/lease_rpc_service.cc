/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    lease_rpc_service.cc
* Author: 
* Date:         2017/07/11
* Version:      1.0
* Description:
* 
***********************************************/
#include "lease_rpc_service.h"
#include "log/log.h"
using grpc::Status;

LeaseRpcSrv::LeaseRpcSrv(std::shared_ptr<KVApi> kv_ptr):
                              kv_ptr_(kv_ptr) {
}

LeaseRpcSrv::~LeaseRpcSrv() {
}

::grpc::Status LeaseRpcSrv::UpdateLease(::grpc::ServerContext* context,
                           const UpdateLeaseReq* request,
                           UpdateLeaseAck* response) {
    const string& key = request->key();
    const string& value = request->value();
    auto metas = request->metas();

    std::map<string,string> map;
    for(auto it = metas.begin(); it != metas.end(); it++) {
        map.insert({it->first,it->second});
    }
    StatusCode ret = kv_ptr_->put_object(key.c_str(), &value,&map);
    if(ret == StatusCode::sOk){
        response->set_status(StatusCode::sOk);
    }
    else {
        LOG_ERROR << "update lease failed:" << key;
        response->set_status(StatusCode::sInternalError);
    }
    return Status::OK;
}

