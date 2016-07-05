/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    writer_service.cc
* Author: 
* Date:         2016/07/12
* Version:      1.0
* Description:
* 
**********************************************/
#include <grpc++/grpc++.h>
#include "writer_service.h"

Status WriterServiceImpl::GetWriteableJournals(ServerContext* context, const GetWriteableJournalsRequest* request,
        GetWriteableJournalsResponse* reply){
    reply->set_result(huawei::proto::RPC_OK);
    std::string s = "journals/vol_1/journal_1";
    std::string *journal = reply->add_journals();
    *journal = s;
    return Status::OK;
}
