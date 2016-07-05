
#include <iostream>
#include <memory>
#include <string>
#include <list>

#include <grpc++/grpc++.h>

#include "src/include/rpc/writer.grpc.pb.h"

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
  std::list<std::string> GetWriteableJournals(const std::string& uuid, const std::string& vol) {
    // Data we are sending to the server.
    GetWriteableJournalsRequest request;
    request.set_uuid(uuid);
    request.set_vol_id(vol);

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

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  WriterClient greeter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  std::string uuid("wr_ab324fd234039545");
  std::string vol_id("test_vol_1");
  std::list<std::string>  list = greeter.GetWriteableJournals(uuid,vol_id);
  if(list.empty()) {
	std::cout << "Greeter received nothing! " << std::endl;
	return 0;
  }
  std::cout << "Greeter received: " << list.front() << std::endl;

  return 0;
}
