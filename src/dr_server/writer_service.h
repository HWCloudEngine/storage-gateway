/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    writer_service.h
* Author: 
* Date:         2016/07/12
* Version:      1.0
* Description:
* 
**********************************************/
#ifndef WRITER_SERVICE_H_
#define WRITER_SERVICE_H_
#include <grpc++/grpc++.h>
#include "rpc/writer.grpc.pb.h"

using grpc::Status;
using grpc::ServerContext;
using huawei::proto::Writer;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
class WriterServiceImpl final : public Writer::Service {
public:
    Status GetWriteableJournals(ServerContext* context, const GetWriteableJournalsRequest* request,
                  GetWriteableJournalsResponse* reply);
};

#endif
