#ifndef WRITER_CLIENT_HPP
#define WRITER_CLIENT_HPP
#include <map>
#include <grpc++/grpc++.h>
#include "../writer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::Writer;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::JournalMarker;
using huawei::proto::UpdateProducerMarkerRequest;
using huawei::proto::UpdateProducerMarkerResponse;
using huawei::proto::UpdateMultiProducerMarkersRequest;
using huawei::proto::UpdateMultiProducerMarkersResponse;

class WriterClient {
public:
    WriterClient(std::shared_ptr<Channel> channel)
        : stub_(Writer::NewStub(channel)) {}

    // Assambles the client's payload, sends it and presents the response back
    // from the server.
    bool GetWriteableJournals(const std::string& uuid, const std::string& vol, const int limit,std::list<std::string>& list_) 
    {
        // Data we are sending to the server.
        GetWriteableJournalsRequest request;
        request.set_uuid(uuid);
        request.set_vol_id(vol);
        request.set_limits(limit);

        // Container for the data we expect from the server.
        GetWriteableJournalsResponse reply;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // The actual RPC.
        Status status = stub_->GetWriteableJournals(&context, request, &reply);

        // Act upon its status.
        RESULT result = reply.result();
        if (status.ok() && (result == DRS_OK)) 
        {
            for(int i=0; i<reply.journals_size(); i++)
            {
                list_.push_back( reply.journals(i));
            }
            return true;
        } 
        else 
        {
            return false;
        }
    }

    bool SealJournals(const std::string& uuid, const std::string& vol, const std::list<std::string>& list_)
    {
          // Data we are sending to the server.
        SealJournalsRequest request;
        request.set_uuid(uuid);
        request.set_vol_id(vol);
        std::list<std::string>::const_iterator  iter;
        for (iter = list_.begin();iter != list_.end();++iter)
        {
            request.add_journals(*iter);
        }

        // Container for the data we expect from the server.
        SealJournalsResponse reply;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // The actual RPC.
        Status status = stub_->SealJournals(&context, request, &reply);

        // Act upon its status.
        RESULT result = reply.result();
        if (status.ok() && (result == DRS_OK)) 
        {
            return true;
        } 
        else 
        {
            return false;
        }
    }

    bool update_producer_marker(const std::string& uuid,
            const std::string& vol, const JournalMarker& marker){
        UpdateProducerMarkerRequest request;
        request.set_uuid(uuid);
        request.set_vol_id(vol);
        request.mutable_marker()->CopyFrom(marker);

        UpdateProducerMarkerResponse reply;
        ClientContext context;
        Status status = stub_->UpdateProducerMarker(&context,request,&reply);
        if(status.ok() && reply.result() == DRS_OK){
            return true;
        }
        else{
            return false;
        }
    }

    bool update_multi_producer_markers(const std::string& uuid,
            const std::map<std::string,JournalMarker>& markers){
        UpdateMultiProducerMarkersRequest request;
        request.set_uuid(uuid);
        for(auto it=markers.begin();it!=markers.end();it++){
            (*request.mutable_markers())[it->first] = it->second;
        }

        UpdateMultiProducerMarkersResponse reply;
        ClientContext context;
        Status status = stub_->UpdateMultiProducerMarkers(&context,request,&reply);
        if(status.ok() && reply.result() == DRS_OK){
            return true;
        }
        else{
            return false;
        }
    }

private:
    std::unique_ptr<Writer::Stub> stub_;
};
#endif
