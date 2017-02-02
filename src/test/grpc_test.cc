/**********************************************
  * Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
  * 
  * File name:    grpc_test.cc
  * Author: 
  * Date:         2016/08/13
  * Version:      1.0
  * Description:
  ***********************************************/
#include <iostream>
#include <memory>
#include <string>
#include <list>
#include <grpc++/grpc++.h>
#include <boost/test/unit_test.hpp> // boost.test header
#include "writer.grpc.pb.h"
#include "consumer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::Writer;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
using huawei::proto::Consumer;
using huawei::proto::JournalMarker;
using huawei::proto::JournalElement;
using huawei::proto::GetJournalMarkerRequest;
using huawei::proto::GetJournalMarkerResponse;
using huawei::proto::GetJournalListRequest;
using huawei::proto::GetJournalListResponse;
using huawei::proto::UpdateConsumerMarkerRequest;
using huawei::proto::UpdateConsumerMarkerResponse;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::CONSUMER_TYPE;
using huawei::proto::REPLICATOR;
using huawei::proto::REPLAYER;
using std::string;

class WriterClient {
public:
    WriterClient(std::shared_ptr<Channel> channel)
        : stub_(Writer::NewStub(channel)) {}

    // Assambles the client's payload, sends it and presents the response back
    // from the server.
    RESULT GetWriteableJournals(const std::string uuid, const std::string vol,
            const int limit,std::list<std::string> &list) {
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
        if (status.ok() && reply.result()==DRS_OK) {
            for(int i=0; i<reply.journals_size(); i++) {
                list.push_back( reply.journals(i));
            }
            return DRS_OK;
        } else {
            return INTERNAL_ERROR;
        }
    }
    RESULT SealJournals(const std::string uuid, const std::string vol,
            const int count, const std::string journal[]) {
        SealJournalsRequest request;
        request.set_uuid(uuid);
        request.set_vol_id(vol);
        for(int i=0;i<count;i++){
            request.add_journals(journal[i]);
        }
        SealJournalsResponse reply;
        ClientContext context;
        Status status = stub_->SealJournals(&context, request, &reply);
        if(!status.ok()){
            BOOST_ERROR("SealJournals rpc call failed!");
            return INTERNAL_ERROR;
        }
        if(DRS_OK != reply.result()){
            BOOST_ERROR("Seal Journals failed!");
            return INTERNAL_ERROR;
        }
        return DRS_OK;      
    }
private:
    std::unique_ptr<Writer::Stub> stub_;
};

struct F {
    F() {
    // setup: make some preconditions before any testCase        
        client = new WriterClient(grpc::CreateChannel("localhost:50051",
                grpc::InsecureChannelCredentials()));
        if(client == nullptr) {
            BOOST_REQUIRE_MESSAGE(false,"creat WriterClient failed!");
        }
    }
    ~F() {
    // teardown: do some cleanup after the testCase
        if(client != nullptr) {
            delete client;
        }
    }
    WriterClient *client;
};

class ConsumerClient{
public:
    ConsumerClient(std::shared_ptr<Channel> channel)
            :stub_(Consumer::NewStub(channel)) {}
    RESULT GetJournalMarker(const string vol,const string uuid,
            const CONSUMER_TYPE type,JournalMarker&marker){
        GetJournalMarkerRequest req;
        ClientContext context;
        GetJournalMarkerResponse res;
        req.set_uuid(uuid);
        req.set_vol_id(vol);
        req.set_type(type);
        Status status = stub_->GetJournalMarker(&context,req,&res);
        if(status.ok() && DRS_OK==res.result()){
            marker.CopyFrom(res.marker());
            return DRS_OK;
        }
        return INTERNAL_ERROR;
    }
    RESULT GetJournalList(const string vol,const string uuid,const CONSUMER_TYPE type,
            const JournalMarker marker,std::list<JournalElement> &list){
        GetJournalListRequest req;
        ClientContext context;
        GetJournalListResponse res;
        req.set_uuid(uuid);
        req.set_vol_id(vol);
        req.set_type(type);
        if(!marker.IsInitialized())
            return INTERNAL_ERROR;
        (req.mutable_marker())->CopyFrom(marker);
        Status status = stub_->GetJournalList(&context,req,&res);
        if(status.ok() && DRS_OK==res.result()){
            for(int i=0;i<res.journals_size();++i)
                list.push_back(res.journals(i));
            return DRS_OK;
        }
        return INTERNAL_ERROR;
    }
    // update consumer maker when time out or comsumed a batch of logs
    RESULT UpdateConsumerMarker(const string vol,const string uuid,
            const CONSUMER_TYPE type,const JournalMarker marker){
        UpdateConsumerMarkerRequest req;
        ClientContext context;
        UpdateConsumerMarkerResponse res;
        req.set_uuid(uuid);
        req.set_vol_id(vol);
        req.set_type(type);
        if(!marker.IsInitialized())
            return INTERNAL_ERROR;
        (req.mutable_marker())->CopyFrom(marker);
        Status status = stub_->UpdateConsumerMarker(&context,req,&res);
        if(status.ok() && DRS_OK==res.result()){
            return DRS_OK;
        }
        return INTERNAL_ERROR;
    }
private:
    std::unique_ptr<Consumer::Stub> stub_;
};

BOOST_AUTO_TEST_SUITE(writer_client_test)
BOOST_AUTO_TEST_CASE(case1){ // test case without fixture
    WriterClient greeter(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
    std::string uuid("wr_ab324fd234039545");
    std::string vol_id("test_vol_1");
    int limit = 1;
    std::list<std::string>  list;
    RESULT res = greeter.GetWriteableJournals(uuid,vol_id,limit,list);
    BOOST_REQUIRE(res == DRS_OK);
    BOOST_WARN(list.size() == limit); // get a warn message if predicate value is not true
    BOOST_CHECK(list.size() > 0); // get a error message if predicate value is not true
    BOOST_REQUIRE(list.empty() != true); // get a fatal error if test failed and assert this test case
    // if you want to print the test message, add argument "--log_level=message" when run the test
    BOOST_TEST_MESSAGE("Greeter received: " << list.front()); // output a boost meesage into test log
}

BOOST_FIXTURE_TEST_CASE(case2, F) { // test case with fixture F
    std::string vol("test_vol_xvde");
    std::string uuid("node1_pid_234");
    std::list<string> list;
    int limit = 5;
    RESULT res = client->GetWriteableJournals(uuid,vol,limit,list);
    BOOST_REQUIRE(res == DRS_OK);// get a fatal error if test failed and assert this test case
    BOOST_CHECK_MESSAGE(list.size() == limit, "get journals count is not match limit.");

    string journals[5];
    int i=0;
    for(auto it=list.begin();it!=list.end() && i<5;++i,++it){
        journals[i] = *it;
    }
    res = client->SealJournals(uuid,vol,i,journals);
    BOOST_REQUIRE(res == DRS_OK);
}
BOOST_FIXTURE_TEST_CASE(case3, F) {
    std::string vol("test_vol_xvde");
    std::string uuid("node1_pid_235");
    std::list<string> list;
    int limit = 1;
    RESULT res = client->GetWriteableJournals(uuid,vol,limit,list);
    BOOST_REQUIRE(res == DRS_OK);    
    BOOST_REQUIRE(list.size() > 0);
    BOOST_CHECK_MESSAGE(list.size() == limit, "get journals count is not match limit.");
}
BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(consumer_client_test)
BOOST_AUTO_TEST_CASE(case4){// test case without fixture
    ConsumerClient consumer(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
    string vol("test_vol_xvde");
    string uuid("node1_pid_234");
    JournalMarker marker;
    RESULT res = consumer.GetJournalMarker(vol,uuid,REPLAYER,marker);
    if(res != DRS_OK){
        BOOST_ERROR("get journal marker failed!"); // increase boost error case count and output an error message
        string key = "/journals/" + vol + "/00000000";
        marker.set_cur_journal(key);
        marker.set_pos(0);
        res = consumer.UpdateConsumerMarker(vol,uuid,REPLAYER,marker);
        BOOST_REQUIRE(res == DRS_OK);
    }
    std::list<JournalElement> list;
    res = consumer.GetJournalList(vol,uuid,REPLAYER,marker,list);
    if(res != DRS_OK){
        BOOST_FAIL("get journal list failed!"); // assert and output message to boost log
    }
    if(list.size() > 0){
        marker.set_cur_journal(list.back().journal());
        marker.set_pos(10240);
        res = consumer.UpdateConsumerMarker(vol,uuid,REPLAYER,marker);
        BOOST_REQUIRE(res == DRS_OK);
        JournalMarker __marker;
        res = consumer.GetJournalMarker(vol,uuid,REPLAYER,__marker);
        BOOST_REQUIRE(res == DRS_OK);
        BOOST_REQUIRE_EQUAL(marker.pos(),__marker.pos());
        BOOST_REQUIRE(0==marker.cur_journal().compare(__marker.cur_journal()));
    }
}

BOOST_AUTO_TEST_CASE(case5){// test case without fixture
    ConsumerClient consumer(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
    string vol("test_vol_xvde");
    string uuid("node1_pid_234");
    JournalMarker marker;
    RESULT res = consumer.GetJournalMarker(vol,uuid,REPLICATOR,marker);
    if(res != DRS_OK){
        BOOST_ERROR("get journal marker failed!"); // increase boost error case count and output an error message
        string key = "/journals/" + vol + "/00000000";
        marker.set_cur_journal(key);
        marker.set_pos(0);
        res = consumer.UpdateConsumerMarker(vol,uuid,REPLICATOR,marker);
        BOOST_REQUIRE(res == DRS_OK);
    }
    
    std::list<JournalElement> list;
    res = consumer.GetJournalList(vol,uuid,REPLICATOR,marker,list);
    if(res != DRS_OK){
        BOOST_FAIL("get journal list failed!"); // assert and output message to boost log
    }
    if(list.size() > 0){
        marker.set_cur_journal(list.back().journal());
        marker.set_pos(10240);
        res = consumer.UpdateConsumerMarker(vol,uuid,REPLICATOR,marker);
        BOOST_REQUIRE(res == DRS_OK);
        JournalMarker __marker;
        res = consumer.GetJournalMarker(vol,uuid,REPLICATOR,__marker);
        BOOST_REQUIRE(res == DRS_OK);
        BOOST_CHECK_MESSAGE(__marker.IsInitialized(),"current journal is not init!");
        BOOST_REQUIRE_EQUAL(marker.pos(),__marker.pos());
        BOOST_REQUIRE(0==marker.cur_journal().compare(__marker.cur_journal()));
    }
}


BOOST_AUTO_TEST_SUITE_END()
