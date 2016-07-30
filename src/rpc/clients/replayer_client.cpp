/*
 * replayer_client.cpp
 *
 *  Created on: 2016Äê7ÔÂ29ÈÕ
 *      Author: smile-luobin
 */

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "replayer_client.hpp"

ReplayerClient::ReplayerClient(std::shared_ptr<Channel> channel) :
        stub_(Writer::NewStub(channel)),
        lexical_cast<string>(uuid(boost::uuids::random_generator()())),
        consumer_type(0) {
}

bool ReplayerClient::GetJournalMarker(const std::string& vol_id,
        JournalMarker& marker_) {
    GetJournalMarkerRequest request;
    request.set_uuid(uuid);
    request.set_vol_id(vol_id);

    GetJournalMarkerResponse reply;
    ClientContext context;

    Status status = stub_->GetJounralMarker(&context, request, &reply);
    if (status.ok() && (result == DRS_OK)) {
        marker_ = reply.marker;
        return true;
    } else {
        return false;
    }
}

bool ReplayerClient::GetJournalList(const std::string& vol_id,
        const JournalMarker& marker, int limit,
        std::list<std::string>& journal_list_) {
    GetJournalListRequest request;
    request.set_limit(limit);
    request.set_vol_id(vol_id);
    request.set_allocated_marker(marker);

    GetJournalListResponse reply;
    ClientContext context;

    Status status = stub_->GetJournalList(&context, request, &reply);
    if (status.ok() && (result == DRS_OK)) {
        for (int i = 0; i < reply.journals_size(); i++) {
            journal_list_.push_back(reply.journals(i));
        }
        return true;
    } else {
        return false;
    }
}

bool ReplayerClient::UpdateConsumerMarker(const std::string& vol_id) {
    UpdateConsumerMarkerRequest request;
    request.set_uuid(uuid);
    request.set_vol_id(vol_id);
    request.set_type(consumer_type);

    UpdateConsumerMarkerResponse reply;
    ClientContext context;

    Status status = stub_->UpdateConsumerMarker(&context, request, &reply);
    if (status.ok() && (result == DRS_OK)){
        return true;
    }else{
        return false;
    }
}
