/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    consumer_service.h
* Author: 
* Date:         2016/07/12
* Version:      1.0
* Description:
* 
**********************************************/
#ifndef CONSUMER_SERVICE_H_
#define CONSUMER_SERVICE_H_
#include <grpc++/grpc++.h>
#include "rpc/consumer.grpc.pb.h"
using grpc::Status;
using grpc::ServerContext;
using huawei::proto::Consumer;
using huawei::proto::GetJournalMarkerRequest;
using huawei::proto::GetJournalMarkerResponse;

class ReplayerServiceImpl final : public Consumer::Service {
    Status GetJournalMarker(ServerContext* context, const GetJournalMarkerRequest* request,
            GetJournalMarkerResponse* reply) override;
};
#endif
