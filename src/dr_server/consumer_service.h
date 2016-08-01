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
#include "journal_meta_manager.h"
using grpc::Status;
using grpc::ServerContext;
using huawei::proto::Consumer;
using huawei::proto::GetJournalMarkerRequest;
using huawei::proto::GetJournalMarkerResponse;
using huawei::proto::GetJournalListRequest;
using huawei::proto::GetJournalListResponse;
using huawei::proto::UpdateConsumerMarkerRequest;
using huawei::proto::UpdateConsumerMarkerResponse;

class ConsumerServiceImpl final : public Consumer::Service {
private:
    JournalMetaManager *_meta;
public:
    ConsumerServiceImpl(JournalMetaManager *meta);
    Status GetJournalMarker(ServerContext* context, const GetJournalMarkerRequest* request,
            GetJournalMarkerResponse* reply) override;
    Status GetJournalList(ServerContext* context, const GetJournalListRequest* request,
            GetJournalListResponse* response) override;
    // update consumer maker when time out or comsumed a batch of logs
    Status UpdateConsumerMarker(ServerContext* context,
            const UpdateConsumerMarkerRequest* request, UpdateConsumerMarkerResponse* response) override;
};
#endif
