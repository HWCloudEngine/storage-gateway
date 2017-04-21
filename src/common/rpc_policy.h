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
#ifndef SRC_COMMON_RPC_POLICY_H_
#define SRC_COMMON_RPC_POLICY_H_
#include <string>

class RpcPolicy {
 public:
     virtual ~RpcPolicy() = default;

     virtual std::string pick_host(const std::string& vname) = 0;
};

class DhtRpcPolicy : public RpcPolicy {
 public:
     explicit DhtRpcPolicy(const std::string& tooz_client);
     ~DhtRpcPolicy() = default;

     std::string pick_host(const std::string& vname) override;

 private:
     std::string tooz_client_;
};

#endif  // SRC_COMMON_RPC_POLICY_H_
