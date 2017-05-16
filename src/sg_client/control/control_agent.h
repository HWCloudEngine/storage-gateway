#ifndef _CONTROL_AGENT_H_
#define _CONTROL_AGENT_H_

#include <list>
#include <string>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/clients/volume_inner_ctrl_client.h"
#include "rpc/volume_control.pb.h"
#include "rpc/volume_control.grpc.pb.h"
#include "log/log.h"
#include "common/agent_ioctl.h"
#include "control_volume.h"


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace huawei::proto;

class AgentControlImpl final: public VolumeControlBase
{

public:
    AgentControlImpl(const std::string& host, const std::string& port,
            std::shared_ptr<VolInnerCtrlClient> vol_inner_client);
    virtual ~AgentControlImpl();
    Status EnableSG(ServerContext* context, const control::EnableSGReq* req,
            control::EnableSGRes* res);
    Status DisableSG(ServerContext* context, const control::DisableSGReq* req,
            control::DisableSGRes* res);
    bool recover_targets();

private:
    bool init();
    bool agent_add_device(std::string vol_name,std::string device);
    bool agent_del_device(std::string device);
    bool persist_device(std::string vol_name,std::string device);
    bool delete_device(std::string device);
    bool agent_device_recover();
    
private:
    int fd_;
    std::string host_;
    std::string port_;
    std::shared_ptr<VolInnerCtrlClient> vol_inner_client_;

};

#endif
