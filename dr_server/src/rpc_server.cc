
#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "include/rpc/writer.grpc.pb.h"
#include "include/rpc/consumer.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::Writer;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
using huawei::proto::Consumer;
using huawei::proto::GetJournalMarkerRequest;
using huawei::proto::GetJournalMarkerResponse;
using huawei::proto::JournalMarker;
// Logic and data behind the server's behavior.
class WriterServiceImpl final : public Writer::Service {
  Status GetWriteableJournals(ServerContext* context, const GetWriteableJournalsRequest* request,
                  GetWriteableJournalsResponse* reply) override {
    reply->set_result(huawei::proto::RPC_OK);
	std::string s = "journals/vol_1/journal_1";
	std::string *journal = reply->add_journals();
	*journal = s;
    return Status::OK;
  }
};

class ReplayerServiceImpl final : public Consumer::Service {
  Status GetJournalMarker(ServerContext* context, const GetJournalMarkerRequest* request,
                  GetJournalMarkerResponse* reply) override {
    JournalMarker *marker = reply->mutable_marker();
    std::cout << "get volume:" << request->vol_id() << " 's marker " << std::endl;
    reply->set_result(huawei::proto::RPC_OK);
    std::string s = "journals/vol_1/journal_1";
    marker->set_cur_journal(s);
    marker->set_pos(0);

    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  WriterServiceImpl service;
  ReplayerServiceImpl replayerSer;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  builder.RegisterService(&replayerSer);

  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
