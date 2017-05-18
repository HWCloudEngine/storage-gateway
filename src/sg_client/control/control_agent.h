#ifndef _CONTROL_AGENT_H_
#define _CONTROL_AGENT_H_

#include <list>
#include <map>
#include <string>
#include "common/config.h"
#include "log/log.h"
#include "common/agent_ioctl.h"

class AgentControl
{
public:
    AgentControl(const std::string& host, const std::string& port);
    ~AgentControl();
    bool attach_volume(const std::string& vol_name, const std::string& device);
    bool detach_volume(const std::string& vol_name, const std::string& device);
    bool recover_targets();
private:
    bool init();
    bool agent_add_device(std::string vol_name,std::string device);
    bool agent_del_device(std::string device);
    bool persist_device(std::string vol_name,std::string device);
    bool delete_device(std::string device);
    bool agent_device_recover();

    int fd_;
    std::string host_;
    std::string port_;
    std::map<std::string, std::string> agent_vols;
};

#endif
