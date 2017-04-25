/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    transaction.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot simple transaction
*  
*************************************************/
#ifndef SRC_SG_CLIENT_SNAPSHOT_TRANSACTION_H_
#define SRC_SG_CLIENT_SNAPSHOT_TRANSACTION_H_
#include <string>
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
using huawei::proto::StatusCode;
using huawei::proto::SnapReqHead;

class ITransaction {
 public:
    ITransaction() = default;
    virtual ~ITransaction() {
    }

    virtual StatusCode create_transaction(const SnapReqHead& shead,
                                          const std::string& snap_name) = 0;
    virtual StatusCode delete_transaction(const SnapReqHead& shead,
                                          const std::string& snap_name) = 0;
    virtual StatusCode rollback_transaction(const SnapReqHead& shead,
                                            const std::string& snap_name) = 0;
};

#endif  // SRC_SG_CLIENT_SNAPSHOT_TRANSACTION_H_
