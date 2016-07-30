/*
 * replayer_client.hpp
 *
 *  Created on: 2016Äê7ÔÂ29ÈÕ
 *      Author: smile-luobin
 */

#ifndef RPC_REPLAYER_CLIENT_HPP_
#define RPC_REPLAYER_CLIENT_HPP_

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

class ReplayerClient {
public:
    ReplayerClient(std::shared_ptr<Channel> channel);
    bool GetJournalMarker(const std::string& vol_id, JournalMarker& marker_);
    bool GetJournalList(const std::string& vol_id, const JournalMarker& marker,
            int limit, std::list<std::string>& journal_list_);
    bool UpdateConsumerMarker(const JournalMarker& marker, const std::string& vol_id);
private:
    std::unique_ptr<Consumer::Stub> stub_;
    CONSUMER_TYPE consumer_type;
    std::string uuid;
};

#endif /* RPC_REPLAYER_CLIENT_HPP_ */
