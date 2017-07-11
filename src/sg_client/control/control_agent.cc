#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <fstream>
#include "control_agent.h"
#include "common/config_option.h"
#include "common/env_posix.h"
#include <boost/tokenizer.hpp>

AgentControl::AgentControl(const std::string& host, const std::string& port) :
        host_(host), port_(port)
{
    if (Env::instance()->file_exists(AGENT_CTL_DEVICE_PATH)) {
        fd_ = open(AGENT_CTL_DEVICE_PATH, O_RDWR);
        if(fd_ == -1)
        {
            LOG_INFO << "open agent ctl device failed,errno:" <<errno;
        }
        init();
    }
}

AgentControl::~AgentControl()
{
    if (fd_ != -1)
    {
        close(fd_);
    }
}

bool AgentControl::init()
{
    struct agent_init init_info;
    init_info.pid = getpid();
    init_info.host = strdup(host_.c_str());
    init_info.port = std::stoi(port_);
    int result = ioctl(fd_, AGENT_INIT, &init_info);
    free(init_info.host);
    if(result != 0)
    {
        LOG_INFO << " agent ctl init failed,errno:" <<errno;
        return false;
    }
    //create agent conf file if not exist
    std::ofstream fc(g_option.local_agent_dev_conf,std::ios::app);
    fc.close();
    LOG_INFO << " agent init OK";
    return true;
}

bool AgentControl::recover_targets()
{
    if(false == agent_device_recover())
    {
        return false;
    }
    return true;
}

bool AgentControl::agent_add_device(std::string vol_name, std::string device,
                                    bool recover)
{
    if(device.empty() || vol_name.empty())
    {
        LOG_INFO << "agent add device failed,dev_path:" << device
                 << "vol_name:" << vol_name;
        return false;
    }
    auto it = agent_vols.find(vol_name);
    if(it != agent_vols.end())
    {
        LOG_INFO << "vol " << vol_name << " is already added";
        return true;
    }
    struct agent_ioctl_add_dev info;
    info.dev_path = strdup(device.c_str());
    info.vol_name= strdup(vol_name.c_str());;
    int result = ioctl(fd_, AGENT_ADD_DEVICE, &info);
    free(info.dev_path);
    free(info.vol_name);
    if(result != 0)
    {
        LOG_INFO << "agent add device failed,errno:" << errno;
        return false;
    }
    agent_vols.insert({vol_name, device});
    if(!recover)
    {
        persist_device(vol_name, device);
    }
    return true;
}

bool AgentControl::agent_del_device(std::string device)
{
    if(device.empty())
    {
        LOG_INFO << "agent del device failed,device is null";
        return false;
    }

    struct agent_ioctl_del_dev info;
    info.dev_path = strdup(device.c_str());
    int result = ioctl(fd_, AGENT_DEL_DEVICE, &info);
    free(info.dev_path);
    if(result != 0)
    {
        LOG_INFO << "agent del device failed,errno:%d" << errno;
        return false;
    }
    return true;
}

bool AgentControl::attach_volume(const std::string& vol_name,
                                 const std::string& device)
{
    LOG_INFO << "attach vol:" << vol_name << "device:" << device;

    if(agent_add_device(vol_name, device, false))
    {
        return true;
    }
    LOG_ERROR << "attach vol:" << vol_name << "device:" << device << " failed";
    return false;
}

bool AgentControl::detach_volume(const std::string& vol_name,
                                 const std::string& device)
{
    LOG_INFO << "detach volume:" << vol_name;
    auto it = agent_vols.find(vol_name);
    if(it == agent_vols.end())
    {
        LOG_INFO << "vol " << vol_name << " is already deleted";
        return true;
    }
    if(agent_del_device(device))
    {
        std::string tmp = vol_name + ":" + device;
        if(delete_device(tmp))
        {
            LOG_INFO << "Delete agent device :" << tmp;
            agent_vols.erase(vol_name);
            return true;
        }
    }
    return false;
}

bool AgentControl::persist_device(const std::string& vol_name, const std::string& device)
{
    std::ofstream f(g_option.local_agent_dev_conf, std::ios::app);
    if(!f.is_open())
    {
        LOG_INFO <<" open agent dev conf file failed";
        return false;
    }
    std::string tmp = vol_name + ":" + device;
    f<< tmp<< std::endl;
    f.close();
    return true;
}

bool AgentControl::delete_device(std::string device)
{
    std::ifstream fin(g_option.local_agent_dev_conf);
    if(!fin.is_open())
    {
        LOG_INFO <<" open agent dev conf file failed";
        return false;
    }
    std::string s((std::istreambuf_iterator<char>(fin)), std::istreambuf_iterator<char>());
    fin.close();
    std::string::size_type pos = s.find(device);
    if(pos == std::string::npos)
    {
        LOG_INFO <<device <<" not found in agent conf file";
        return true;
    }
    s.erase(pos,device.size()+1);
    std::ofstream fout(g_option.local_agent_dev_conf);
    if(!fout.is_open())
    {
        LOG_INFO <<" open agent dev conf file failed";
        return false;
    }
    fout<<s;
    fout.close();
}

bool AgentControl::agent_device_recover()
{
    std::ifstream f(g_option.local_agent_dev_conf);
    if(!f.is_open())
    {
        LOG_INFO <<" open agent dev conf file failed";
        return false;
    }
    std::string s;
    while(getline(f,s))
    {
        boost::char_separator<char> sep(":");
        boost::tokenizer<boost::char_separator<char>> tokens(s, sep);
        std::string dev_path,vol_name;
        for(auto it=tokens.begin();it != tokens.end();++it)
        {
            if(vol_name.empty())
            {
                vol_name=*it;
                continue;
            }
            if(dev_path.empty())
            {
                dev_path=*it;
            }
        }
        if(vol_name.empty() || dev_path.empty())
        {
            LOG_INFO <<" persistent device info is invalid,info:"<<s;
            continue;
        }
        if(false == agent_del_device(dev_path))
        {
            continue;
        }
        agent_add_device(vol_name, dev_path, true);
    }
    f.close();
    return true;
}

