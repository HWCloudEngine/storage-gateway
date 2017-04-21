/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rpc_server.hpp
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:
* 
***********************************************/
#include "rpc_policy.h"

DhtRpcPolicy::DhtRpcPolicy(const std::string& tooz_client) {
    tooz_client_ = tooz_client;
    // todo connect to tooz cluster
}


std::string DhtRpcPolicy::pick_host(const std::string& vname) {
    // todo accord vname to get host address from tooz cluster
    return "";
}
