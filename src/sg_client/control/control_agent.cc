#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <fstream>
#include "control_agent.h"
#include <boost/tokenizer.hpp>


AgentControlImpl::AgentControlImpl(const Configure& conf, const std::string& host,
        const std::string& port, std::shared_ptr<VolInnerCtrlClient> vol_inner_client) :
        conf_(conf), host_(host), port_(port), 
        vol_inner_client_(vol_inner_client),VolumeControlBase(vol_inner_client)
{
    fd_ = open(AGENT_CTL_DEVICE_PATH, O_RDWR);
    if(fd_ == -1)
    {
        LOG_INFO << "open agent ctl device failed,errno:" <<errno;
    }
    init();

}

AgentControlImpl::~AgentControlImpl()
{
    if (fd_ != -1)
    {
        close(fd_);
    }
}

bool AgentControlImpl::init()
{
    struct agent_init init_info;
    init_info.pid = getpid();
    init_info.host = strdup(host_.c_str());
    init_info.port = std::stoi(port_);
    int result = ioctl(fd_,AGENT_INIT,&init_info);
    free(init_info.host);
    if(result != 0)
    {
        LOG_INFO << " agent ctl init failed,errno:" <<errno;
        return false;
    }
    if(false == agent_device_init())
    {
        return false;
    }
    return true;
}

bool AgentControlImpl::agent_add_device(std::string vol_name, std::string device)
{
    if(device.empty() || vol_name.empty())
    {
        LOG_INFO << "agent add device failed,dev_path:"<<device<<"vol_name:"<<vol_name;
        return false;
    }
    struct agent_ioctl_add_dev info;
    info.dev_path = strdup(device.c_str());
    info.vol_name= strdup(vol_name.c_str());;
    int result = ioctl(fd_,AGENT_ADD_DEVICE,&info);
    free(info.dev_path);
    free(info.vol_name);
    if(result != 0)
    {
        LOG_INFO << "agent add device failed,errno:" << errno;
        return false;
    }
    return true;
    
}

bool AgentControlImpl::agent_del_device(std::string device)
{
    if(device.empty())
    {
        LOG_INFO << "agent del device failed,device is null";
        return false;
    }
    struct agent_ioctl_del_dev info;
    info.dev_path = strdup(device.c_str());
    int result = ioctl(fd_,AGENT_DEL_DEVICE,&info);
    free(info.dev_path);
    if(result != 0)
    {
        LOG_INFO << "agent del device failed,errno:%d" << errno;
        return false;
    }
    return true;

}

Status AgentControlImpl::EnableSG(ServerContext* context, const control::EnableSGReq* req,
        control::EnableSGRes* res)
{
    string vol_name = req->volume_id();
    string dev_name = req->device();
    size_t dev_size = req->size();
    
    LOG_INFO << "enable sg vol:" << vol_name << "device:" << dev_name << " size:" << dev_size;

    StatusCode ret = vol_inner_client_->create_volume(vol_name, dev_name, dev_size, VOL_AVAILABLE);
    if(ret != StatusCode::sOk){
        res->set_status(StatusCode::sInternalError);
        LOG_ERROR << "enable sg vol:" << vol_name << " device:" << dev_name << " failed"; 
        return Status::OK;
    }

    if(agent_add_device(vol_name, dev_name))
    {
        persist_device(vol_name,dev_name);
        res->set_status(StatusCode::sOk);
        return Status::OK;
     }
    LOG_ERROR << "enable sg failed,vol:" << vol_name << "device:" << dev_name;
    vol_inner_client_->delete_volume(vol_name);
    res->set_status(StatusCode::sInternalError);
    return Status::CANCELLED;
}

Status AgentControlImpl::DisableSG(ServerContext* context, const control::DisableSGReq* req,
        control::DisableSGRes* res)
{
    std::string volume_id = req->volume_id();
    VolumeInfo volume;
    vol_inner_client_->get_volume(volume_id, volume);
    std::string device = volume.path();
    
    LOG_INFO << "Disable sg volume:" << volume_id;

    if(agent_del_device(device))
    {
        delete_device(device);
        LOG_INFO << "Delete agent device :" << device;
        if(StatusCode::sOk == (vol_inner_client_->delete_volume(volume_id)))
        {
            res->set_status(StatusCode::sOk);
            return Status::OK;
        }
    }
    
    LOG_ERROR << "DisableSG failed,vol:" << volume_id;
    res->set_status(StatusCode::sInternalError);
    return Status::CANCELLED;
}

bool AgentControlImpl::persist_device(std::string vol_name,std::string device)
{
    std::ofstream f(conf_.agent_dev_conf,std::ios::app);
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
bool AgentControlImpl::delete_device(std::string device)
{
    std::ifstream fin(conf_.agent_dev_conf);
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
        return false;
    }
    s.erase(pos,device.size()+1);
    std::ofstream fout(conf_.agent_dev_conf);
    if(!fout.is_open())
    {
        LOG_INFO <<" open agent dev conf file failed";
        return false;
    }
    fout<<s;
    fout.close();
}

bool AgentControlImpl::agent_device_init()
{
    std::ifstream f(conf_.agent_dev_conf);
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
        bool vol_flag = true;
        bool dev_flag = true;
        for(auto it=tokens.begin();it != tokens.end();++it)
        {
            if(vol_flag)
            {
                vol_name=*it;
                vol_flag = false;
                continue;
            }
            if(dev_flag)
            {
                dev_path=*it;
                dev_flag = false;
            }
        }
        if(vol_flag || dev_flag)
        {
            LOG_INFO <<" persistent device info is invalid,info:"<<s;
            continue;
        }
        if(false == agent_del_device(dev_path));
        {
            continue;
        }
        agent_add_device(vol_name, dev_path);
    }
    return true;
}

