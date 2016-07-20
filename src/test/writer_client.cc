/**********************************************
  * Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
  * 
  * File name:    writer_client.cc
  * Author: 
  * Date:         2016/07/13
  * Version:      1.0
  * Description:
  * 
  ***********************************************/
#define BOOST_TEST_DYN_LINK // use dynamic boost libraries
// BOOST_TEST_MODULE macro auto implement initialization function and name the main test suite
#define BOOST_TEST_MODULE write_client_test
#include <iostream>
#include <memory>
#include <string>
#include <list>
#include <grpc++/grpc++.h>
#include <boost/test/unit_test.hpp> // boost.test header
#include "writer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::Writer;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
using huawei::proto::RESULT;

class WriterClient {
public:
    WriterClient(std::shared_ptr<Channel> channel)
        : stub_(Writer::NewStub(channel)) {}

    // Assambles the client's payload, sends it and presents the response back
    // from the server.
    std::list<std::string> GetWriteableJournals(const std::string& uuid, const std::string& vol, const int limit) {
        // Data we are sending to the server.
        GetWriteableJournalsRequest request;
        request.set_uuid(uuid);
        request.set_vol_id(vol);
        request.set_limits(limit);

        // Container for the data we expect from the server.
        GetWriteableJournalsResponse reply;
        std::list<std::string> list;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // The actual RPC.
        Status status = stub_->GetWriteableJournals(&context, request, &reply);

        // Act upon its status.
        if (status.ok()) {
            for(int i=0; i<reply.journals_size(); i++) {
                list.push_back( reply.journals(i));
            }
            return list;
        } else {
            return list;
        }
    }

private:
    std::unique_ptr<Writer::Stub> stub_;
};

struct F {
    F() {
    // setup: make some preconditions before any testCase
        std::string vol("test_vol_xvde");
        std::string uuid("node1_pid_234");
        limit = 1;
        client = new WriterClient(grpc::CreateChannel("localhost:50051",
                grpc::InsecureChannelCredentials()));
        if(client == nullptr) {
            BOOST_REQUIRE_MESSAGE(false,"creat WriterClient failed!");
        }
        list.clear();
        list = client->GetWriteableJournals(uuid,vol,limit);
    }
    ~F() {
    // teardown: do some cleanup after the testCase
        list.clear();
        if(client != nullptr) {
            delete client;
        }
    }
    int limit;
    std::list<std::string> list;
    WriterClient *client;
};

BOOST_AUTO_TEST_SUITE(writer_client_test)
BOOST_AUTO_TEST_CASE(case1){ // test case without fixture
// Instantiate the client. It requires a channel, out of which the actual RPCs
// are created. This channel models a connection to an endpoint (in this case,
// localhost at port 50051). We indicate that the channel isn't authenticated
// (use of InsecureChannelCredentials()).
    WriterClient greeter(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
    std::string uuid("wr_ab324fd234039545");
    std::string vol_id("test_vol_1");
    int limit = 1;
    std::list<std::string>  list = greeter.GetWriteableJournals(uuid,vol_id,limit);
    BOOST_WARN(list.size() == limit); // get a warn message if predicate value is not true
    BOOST_CHECK(list.size() > 0); // get a error message if predicate value is not true
    BOOST_REQUIRE(list.empty() != true); // get a fatal error if test failed and assert this test case
    // if you want to print the test message, add argument "--log_level=message" when run the test
    BOOST_TEST_MESSAGE("Greeter received: " << list.front()); // output a boost meesage into test log
}

BOOST_FIXTURE_TEST_CASE(case2, F) { // test case with fixture F
    BOOST_CHECK_MESSAGE(list.size() == limit, "get journals count is not match limit.");    
}
BOOST_FIXTURE_TEST_CASE(case3, F) {
    BOOST_REQUIRE(list.size() > 0);
}
BOOST_AUTO_TEST_SUITE_END()
