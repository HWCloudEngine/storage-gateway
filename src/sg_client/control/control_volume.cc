/**********************************************
 * Copyright (c) 2017 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * File name:    volume_control.cc
 * Author:
 * Date:         Jan 20, 2017
 * Version:      1.0
 * Description:
 *
 ************************************************/
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <errno.h>
#include <regex.h>
#include <fstream>
#include "common/env_posix.h"
#include "common/config_option.h"
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include "control_volume.h"
#include "volume_manager.h"

using namespace std;
using google::protobuf::Map;
using huawei::proto::StatusCode;
using huawei::proto::VolumeInfo;
using huawei::proto::VOL_AVAILABLE;
using huawei::proto::VOL_ENABLING;
using huawei::proto::ClientMode;

// start class VolumeControlImpl
VolumeControlImpl::VolumeControlImpl(const std::string& host,
                                     const std::string& port,
                                     VolumeManager& vol_manager) :
        host_(host), port_(port),
        vol_manager_(vol_manager)
{
    iscsi_control_ptr = new ISCSIControl(host_, port_);
    agent_control_ptr = new AgentControl(host_, port_);
}

VolumeControlImpl::~VolumeControlImpl()
{
}

bool VolumeControlImpl::execute_cmd(const std::string& command,
                                    std::string& result)
{
    FILE* f;
    char buf[1024];
    result = "";
    if ((f = popen(command.c_str(), "r")) != NULL)
    {
        while (fgets(buf, 1024, f) != NULL)
        {
            result += std::string(buf);
        }
        return true;
    }
    else
    {
        return false;
    }
}

bool VolumeControlImpl::recover_targets()
{
    bool ret = iscsi_control_ptr->recover_targets();
    if(ret == false)
        return false;
    ret = agent_control_ptr->recover_targets();
    return ret;
}

Status VolumeControlImpl::ListDevices(ServerContext* context,
                                      const control::ListDevicesReq* req,
                                      control::ListDevicesRes* res)
{
    //cmd: rescan devices
    LOG_INFO<<"rescan devices";
    std::string cmd = "for f in /sys/class/scsi_host/host*/scan; \
            do echo '- - -' > $f; done";
    int iret = system(cmd.c_str());
    if(iret == -1){
        LOG_ERROR << "scsi scan failed.";
        res->set_status(StatusCode::sInternalError);
        return Status::OK;
    }

    //cmd: list devices
    LOG_INFO<<"list devices";
    std::string devices_info;
    cmd = "lsblk -dn -o name";
    bool ret = execute_cmd(cmd, devices_info);
    if (ret == false)
    {
        res->set_status(StatusCode::sInternalError);
        return Status::OK;
    }
    else
    {
        boost::char_separator<char> sep("\n");
        boost::tokenizer<boost::char_separator<char>> tokens(devices_info, sep);
        for (auto t : tokens)
        {
            res->add_devices()->append("/dev/" + t);
        }
        res->set_status(StatusCode::sOk);
        return Status::OK;
    }
}

Status VolumeControlImpl::GetVolume(ServerContext* context,
                                    const control::GetVolumeReq* req,
                                    control::GetVolumeRes* res)
{
    std::string volume_id = req->volume_id();
    VolumeInfo volume;
    LOG_INFO << "get sg vol:" << volume_id;
    StatusCode ret = g_rpc_client.get_volume(volume_id, volume);
    if (ret == StatusCode::sOk)
    {
        res->mutable_volume()->CopyFrom(volume);
    }
    res->set_status(ret);
    return Status::OK;
}

Status VolumeControlImpl::ListVolumes(ServerContext* context,
                                      const control::ListVolumesReq* req,
                                      control::ListVolumesRes* res)
{
    std::list<VolumeInfo> volumes;
    StatusCode ret = g_rpc_client.list_volume(volumes);
    if (ret == StatusCode::sOk)
    {
        for (auto volume : volumes)
        {
            res->add_volumes()->CopyFrom(volume);
        }
    }
    res->set_status(ret);
    return Status::OK;
}

Status VolumeControlImpl::EnableSG(ServerContext* context,
                                   const control::EnableSGReq* req,
                                   control::EnableSGRes* res)
{
    string vol_name = req->volume_id();
    string dev_name = req->device();
    size_t dev_size = req->size();
    LOG_INFO << "enable sg vol:" << vol_name << " device:" << dev_name;

    VolumeInfo volume;
    StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
    if(ret != StatusCode::sOk)
    {
        StatusCode ret = g_rpc_client.create_volume(
                vol_name, dev_name, dev_size, VOL_AVAILABLE);
        if(ret != StatusCode::sOk)
        {
            LOG_ERROR << "enable sg vol:" << vol_name << " device:" << dev_name << " failed";
            res->set_status(StatusCode::sInternalError);
        }
        else
        {
            StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
            if (ret != StatusCode::sOk)
            {
                LOG_ERROR << "enable sg vol:" << vol_name << " failed";
                res->set_status(ret);
                return Status::OK;
            }
            LOG_INFO << "enable sg vol:" << vol_name << " device:" << dev_name << " succeed";
            vol_manager_.add_volume(volume);
            res->set_status(StatusCode::sOk);
        }
    }
    else
    {
        if(dev_name != volume.path())
        {
            StatusCode ret = g_rpc_client.update_volume_path(vol_name,
                                                                   dev_name);
            if(ret != StatusCode::sOk)
            {
                LOG_ERROR << "enable sg vol:" << vol_name << " failed";
                res->set_status(ret);
                return Status::OK;
            }
        }
        StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
        if (ret != StatusCode::sOk)
        {
            LOG_ERROR << "enable sg vol:" << vol_name << " failed";
            res->set_status(ret);
            return Status::OK;
        }
        LOG_INFO << "enable sg vol:" << vol_name << " device:" << dev_name
                 << " succeed";
        vol_manager_.add_volume(volume);
        res->set_status(StatusCode::sOk);
    }
    return Status::OK;
}

Status VolumeControlImpl::DisableSG(ServerContext* context,
                                    const control::DisableSGReq* req,
                                    control::DisableSGRes* res)
{
    std::string vol_name = req->volume_id();
    LOG_INFO << "disable sg vol:" << vol_name;
    VolumeInfo volume;
    StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
    if (ret != StatusCode::sOk)
    {
        LOG_INFO << "vol:" << vol_name << "is not enabled";
        res->set_status(StatusCode::sOk);
    }
    else
    {
        StatusCode ret = g_rpc_client.delete_volume(vol_name);
        if(ret == StatusCode::sOk)
        {
            LOG_INFO << "disable sg volume:" << vol_name << " succeed";
            vol_manager_.del_volume(vol_name);
            res->set_status(ret);
        }else
        {
            LOG_ERROR << "disable sg volume:" << vol_name << " failed";
            res->set_status(StatusCode::sInternalError);
        }
    }
    return Status::OK;
}

// used to start io-hook(iscsi mode) or del volume (agent mode)
Status VolumeControlImpl::InitializeConnection(ServerContext* context,
                                               const control::InitializeConnectionReq* req,
                                               control::InitializeConnectionRes* res)
{
    std::string vol_name = req->volume_id();
    ClientMode client_mode = req->mode();
    LOG_INFO << "initialize connection vol:" << vol_name << ", mode" << client_mode;
    VolumeInfo volume;
    StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
    if(ret != StatusCode::sOk)
    {
        LOG_ERROR << "initialize connection vol:" << vol_name << " failed";
        res->set_status(ret);
    }
    else
    {
        if (client_mode == ClientMode::ISCSI_MODE)
        {
            std::map <std::string, std::string> connection_info;
            if(iscsi_control_ptr->initialize_connection(vol_name,
                                                        volume.path(),
                                                        connection_info))
            {
                LOG_INFO << "initialize connection vol:" << vol_name << " ok";
                res->set_status(StatusCode::sOk);
                for(auto item: connection_info)
                {
                    (*res->mutable_connection_info())[item.first] = item.second;
                }
            }
            else
            {
                LOG_ERROR << "initialize connection vol:" << vol_name
                          << " failed";
                res->set_status(StatusCode::sInternalError);
            }
        }
        else
        {
            vol_manager_.del_volume(vol_name);
            res->set_status(StatusCode::sOk);
        }
    }
    return Status::OK;
}

// used to stop io-hook(iscsi mode) or update device (agent mode)
Status VolumeControlImpl::TerminateConnection(ServerContext* context,
                                              const control::TerminateConnectionReq* req,
                                              control::TerminateConnectionRes* res)
{
    std::string vol_name = req->volume_id();
    ClientMode client_mode = req->mode();
    std::string device = req->device();
    LOG_INFO << "terminate connection vol:" << vol_name << ", mode:" << client_mode;
    VolumeInfo volume;
    StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
    if(ret != StatusCode::sOk)
    {
        LOG_ERROR << "terminate connection vol:" << vol_name << " failed";
        res->set_status(ret);
    }
    else
    {
        if (client_mode == ClientMode::ISCSI_MODE)
        {
            if(iscsi_control_ptr->terminate_connection(vol_name))
            {
                LOG_INFO << "terminate connection vol:" << vol_name << " ok";
                vol_manager_.deinit_socket(vol_name);
                res->set_status(StatusCode::sOk);
            }
            else
            {
                LOG_ERROR << "terminate connection vol:" << vol_name
                          << " failed";
                res->set_status(StatusCode::sInternalError);
            }
        }
        else
        {
            if(device != volume.path())
            {
                StatusCode ret = g_rpc_client.update_volume_path(vol_name,
                                                                       device);
                if(ret != StatusCode::sOk)
                {
                    LOG_ERROR << "terminate connection vol:" << vol_name << " failed";
                    res->set_status(ret);
                    return Status::OK;
                }
            }
            StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
            if (ret != StatusCode::sOk)
            {
                LOG_ERROR << "terminate connection vol:" << vol_name << " failed";
                res->set_status(ret);
                return Status::OK;
            }
            vol_manager_.add_volume(volume);
            res->set_status(StatusCode::sOk);
        }
    }
    return Status::OK;
}

// used for agent mode to start io-hook and add volume
Status VolumeControlImpl::AttachVolume(ServerContext* context,
                                       const control::AttachVolumeReq* req,
                                       control::AttachVolumeRes* res)
{
    std::string vol_name = req->volume_id();
    std::string device = req->device();
    LOG_INFO << "attach vol:" << vol_name;
    VolumeInfo volume;
    StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
    if(ret != StatusCode::sOk)
    {
        LOG_ERROR << "attach vol:" << vol_name << " failed";
        res->set_status(ret);
    }
    else
    {
        if(device != volume.path())
        {
            StatusCode ret = g_rpc_client.update_volume_path(vol_name, device);
            if(ret != StatusCode::sOk)
            {
                LOG_ERROR << "attach vol:" << vol_name << " failed";
                res->set_status(ret);
                return Status::OK;
            }
        }
        StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
        if(ret != StatusCode::sOk)
        {
            LOG_ERROR << "attach vol:" << vol_name << " failed";
            res->set_status(ret);
        }
        vol_manager_.add_volume(volume);
        if(agent_control_ptr->attach_volume(vol_name, device))
        {
            res->set_status(StatusCode::sOk);
        }
        else
        {
            res->set_status(StatusCode::sInternalError);
        }
    }
    return Status::OK;
}

// used for agent mode to start io-hook and del volume
Status VolumeControlImpl::DetachVolume(ServerContext* context,
                                       const control::DetachVolumeReq* req,
                                       control::DetachVolumeRes* res)
{
    std::string vol_name = req->volume_id();
    LOG_INFO << "attach vol:" << vol_name;
    VolumeInfo volume;
    StatusCode ret = g_rpc_client.get_volume(vol_name, volume);
    if(ret != StatusCode::sOk)
    {
        LOG_ERROR << "detach vol:" << vol_name << " failed";
        res->set_status(ret);
    }
    else
    {
        if(agent_control_ptr->detach_volume(vol_name, volume.path()))
        {
            res->set_status(StatusCode::sOk);
            vol_manager_.del_volume(vol_name);
        }
        else
        {
            res->set_status(StatusCode::sInternalError);
        }
    }
    return Status::OK;
}
//end class VolumeControlImpl
