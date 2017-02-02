/*
 * replayer_client.hpp
 *
 *  Created on: 2016��7��29��
 *      Author: smile-luobin
 */

#ifndef RPC_REPLAYER_CLIENT_H_
#define RPC_REPLAYER_CLIENT_H_

#include <list>
#include <string>
#include <grpc++/grpc++.h>
#include "../consumer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::Consumer;
using huawei::proto::GetJournalListRequest;
using huawei::proto::GetJournalListResponse;
using huawei::proto::GetJournalMarkerRequest;
using huawei::proto::GetJournalMarkerResponse;
using huawei::proto::UpdateConsumerMarkerRequest;
using huawei::proto::UpdateConsumerMarkerResponse;
using huawei::proto::RESULT;
using huawei::proto::CONSUMER_TYPE;
using huawei::proto::REPLAYER;
using huawei::proto::DRS_OK;
using huawei::proto::JournalMarker;
using huawei::proto::JournalElement;

class ReplayerClient {
public:
    ReplayerClient(std::shared_ptr<Channel> channel) :
            stub_(Consumer::NewStub(channel)),
            lease_uuid("lease-uuid"),
            consumer_type(REPLAYER){
    }

    bool GetJournalMarker(const std::string& vol_id, JournalMarker& marker_) {
        GetJournalMarkerRequest request;
        request.set_vol_id(vol_id);
        request.set_uuid(lease_uuid);
        request.set_type(consumer_type);
        GetJournalMarkerResponse reply;
        ClientContext context;

        Status status = stub_->GetJournalMarker(&context, request, &reply);
        RESULT result = reply.result();
        if (status.ok() && (result == DRS_OK)) {
            marker_.CopyFrom(reply.marker());
            return true;
        } else {
            return false;
        }
    }
    bool GetJournalList(const std::string& vol_id, const JournalMarker& marker,
            int limit, std::list<JournalElement>& journal_list_) {
        GetJournalListRequest request;
        request.set_limit(limit);
        request.set_vol_id(vol_id);
        request.set_uuid(lease_uuid);
        request.set_type(consumer_type);
        if (!marker.IsInitialized())
            return false;
        (request.mutable_marker())->CopyFrom(marker);

        GetJournalListResponse reply;
        ClientContext context;

        Status status = stub_->GetJournalList(&context, request, &reply);
        RESULT result = reply.result();
        if (status.ok() && (result == DRS_OK)) {
            for (int i = 0; i < reply.journals_size(); i++) {
                journal_list_.push_back(reply.journals(i));
            }
            return true;
        } else {
            return false;
        }
    }
    bool UpdateConsumerMarker(const JournalMarker& marker,
            const std::string& vol_id) {
        UpdateConsumerMarkerRequest request;
        request.set_uuid(lease_uuid);
        request.set_vol_id(vol_id);
        request.set_type(consumer_type);
        if (!marker.IsInitialized())
            return false;
        (request.mutable_marker())->CopyFrom(marker);

        UpdateConsumerMarkerResponse reply;
        ClientContext context;

        Status status = stub_->UpdateConsumerMarker(&context, request, &reply);
        RESULT result = reply.result();
        if (status.ok() && (result == DRS_OK)) {
            return true;
        } else {
            return false;
        }
    }
private:
    std::unique_ptr<Consumer::Stub> stub_;
    CONSUMER_TYPE consumer_type;
    std::string lease_uuid;
};

#endif /* RPC_REPLAYER_CLIENT_HPP_ */
