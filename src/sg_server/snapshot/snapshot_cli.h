/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_client.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot rpc client for other module
* 
*************************************************/
#ifndef SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_CLI_H_
#define SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_CLI_H_
#include <string>
#include "rpc/clients/snapshot_ctrl_client.h"

SnapshotCtrlClient* create_snapshot_rpc_client(const std::string& vol_name);
void destroy_snapshot_rpc_client(SnapshotCtrlClient* cli);

#endif  //  SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_CLI_H_
