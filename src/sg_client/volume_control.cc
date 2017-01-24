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
#include <fstream>
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include "common/config_parser.h"
#include "volume_control.h"

using huawei::proto::StatusCode;
using huawei::proto::VolumeInfo;
using huawei::proto::VOL_AVAILABLE;
using huawei::proto::VOL_ENABLING;

VolumeControlImpl::VolumeControlImpl(const std::string& host,
        const std::string& port,
        std::shared_ptr<VolInnerCtrlClient> vol_inner_client) :
        host_(host), port_(port), vol_inner_client_(vol_inner_client)
{
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    std::string target_path_ = "/etc/tgt/config.d/";
    target_path_ = parser->get_default("sg_client.target_path",
            target_path_);
    parser.reset();
}

bool VolumeControlImpl::create_volume(const std::string& volume_id, int size,
        const std::string& device)
{
    StatusCode ret = vol_inner_client_->create_volume(volume_id, device, size,
            VOL_ENABLING);
    if (ret == StatusCode::sOk)
    {
        return true;
    }
    else
    {
        return false;
    }
}

bool VolumeControlImpl::delete_volume(const std::string& volume_id)
{
    StatusCode ret = vol_inner_client_->delete_volume(volume_id);
    if (ret == StatusCode::sOk)
    {
        return true;
    }
    else
    {
        return false;
    }
}

bool VolumeControlImpl::update_volume_status(const std::string& volume_id,
        const VOLUME_STATUS& status)
{
    StatusCode ret = vol_inner_client_->update_volume_status(volume_id, status);
    if (ret == StatusCode::sOk)
    {
        return true;
    }
    else
    {
        return false;
    }
}

bool VolumeControlImpl::generate_config(const std::string& volume_id,
        const std::string& device, const std::string& target_iqn,
        std::string& config)
{
    boost::format target_format =
            boost::format(
                    std::string("<target %1%> \n") +
            "\t bs-type hijacker \n" +
            "\t bsopts \"host=%2%\\;port=%3%\\;volume=%4%\\;device=%5%\" \n" +
            "\t backing-store %5% \n" +
            "\t initiator-address ALL \n" +
            "</target>");

    config = str(
            target_format % target_iqn % host_ % port_ % volume_id % device);
    return true;
}

bool VolumeControlImpl::persist_config(const std::string& volume_id,
        const std::string& config)
{
    std::string file_name = target_path_ + volume_id;
    LOG_INFO<<"persist config file "<<file_name;
    std::ofstream f(file_name);
    f << config;
    f.close();
    return true;
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

bool VolumeControlImpl::update_target(const std::string& target_iqn)
{
    LOG_INFO<<"update target "<<target_iqn;
    std::string cmd = "tgt-admin --update " + target_iqn;
    int ret = system(cmd.c_str());
    if (ret == 0)
    {
        return true;
    }
    else
    {
        LOG_INFO<<"update target "<<target_iqn <<"failed.";
        return false;
    }
}

bool VolumeControlImpl::remove_target(const std::string& target_iqn)
{
    LOG_INFO<<"remove target "<<target_iqn;
    std::string cmd = "tgt-admin --force --delete " + target_iqn;
    int ret = system(cmd.c_str());
    if(ret == 0)
    {
        return true;
    }
    else
    {
        LOG_INFO<<"remove target "<<target_iqn <<"failed.";
        return false;
    }
}

bool VolumeControlImpl::remove_config(const std::string& volume_id)
{
    std::string file_name = target_path_ + volume_id;
    LOG_INFO<<"remove config file "<<file_name;
    int ret = remove(file_name.c_str());
    if (ret == 0)
    {
        return true;
    }
    else
    {
        LOG_INFO<<"remove config file "<<file_name<<"failed";
        return false;
    }
}

bool VolumeControlImpl::remove_device(const std::string& device)
{
    LOG_INFO<<"remove device "<<device;
    std::string cmd = "blockdev --flushbufs " + device;
    system(cmd.c_str());

    std::string path = "/sys/block/" + device.replace(0, 5, "") + "/device/delete";
    cmd = "echo 1 | tee -a " + path;
    system(cmd.c_str());
    return true;
}

Status VolumeControlImpl::ListDevices(ServerContext* context,
        const control::ListDevicesReq* req, control::ListDevicesRes* res)
{
    //cmd: rescan devices
    LOG_INFO<<"rescan devices";
    std::string cmd = "for f in /sys/class/scsi_host/host*/scan; \
            do echo '- - -' > $f; done";
    system(cmd.c_str());

    //cmd: list devices
    LOG_INFO<<"list devices";
    std::string devices_info;
    cmd = "lsblk -dn -o name";
    bool ret = execute_cmd(cmd, devices_info);
    if (ret == false)
    {
        res->set_status(StatusCode::sInternalError);
        return Status::CANCELLED;
    }
    else
    {
        boost::char_separator<char> sep("\n");
        boost::tokenizer<boost::char_separator<char>> tokens(devices_info, sep);
        for (auto t : tokens)
        {
            res->add_devices()->append("/dev/" + t);
        }
        res->set_status(StatusCode::sInternalError);
        return Status::OK;
    }
}

Status VolumeControlImpl::EnableSG(ServerContext* context,
        const control::EnableSGReq* req, control::EnableSGRes* res)
{
    std::string volume_id = req->volume_id();
    std::string device = req->device();
    std::string target_iqn = req->target_iqn();
    int size = req->size();

    //step 1: create volume
    bool result = create_volume(volume_id, size, device);
    if (result)
    {
        //step 2: persist config and update target
        std::string config;
        generate_config(volume_id, device, target_iqn, config);
        if (persist_config(volume_id, config))
        {
            if (update_target(target_iqn))
            {
                //step 3: update status
                update_volume_status(volume_id, VOL_AVAILABLE);
                res->set_status(StatusCode::sOk);
                return Status::OK;
            }
            else
            {
                remove_config(volume_id);
            }
        }
    }
    else
    {
        res->set_status(StatusCode::sInternalError);
        return Status::CANCELLED;
    }
}

Status VolumeControlImpl::DisableSG(ServerContext* context,
        const control::DisableSGReq* req, control::DisableSGRes* res)
{
    std::string volume_id = req->volume_id();
    std::string target_iqn = req->target_iqn();

    VolumeInfo volume;
    vol_inner_client_->get_volume(volume_id, volume);
    remove_device(volume.path());

    if (remove_config(volume_id))
    {
        if (remove_target(target_iqn))
        {
            bool ret = delete_volume(volume_id);
            if (ret == true)
            {
                res->set_status(StatusCode::sOk);
                return Status::OK;
            }
            else
            {
                update_target(target_iqn);
            }
        }
        else
        {
            std::string config;
            generate_config(volume_id, volume.path(), target_iqn, config);
            persist_config(volume_id, config);
        }
    }

    res->set_status(StatusCode::sInternalError);
    return Status::CANCELLED;
}

Status VolumeControlImpl::GetVolume(ServerContext* context,
        const control::GetVolumeReq* req, control::GetVolumeRes* res)
{
    std::string volume_id = req->volume_id();
    VolumeInfo volume;
    StatusCode ret = vol_inner_client_->get_volume(volume_id, volume);
    if (ret == StatusCode::sOk)
    {
        res->set_status(ret);
        res->mutable_volume()->CopyFrom(volume);
        return Status::OK;
    }
    else
    {
        res->set_status(ret);
        return Status::CANCELLED;
    }
}

Status VolumeControlImpl::ListVolumes(ServerContext* context,
        const control::ListVolumesReq* req, control::ListVolumesRes* res)
{
    std::list<VolumeInfo> volumes;
    StatusCode ret = vol_inner_client_->list_volume(volumes);
    if (ret == StatusCode::sOk)
    {
        res->set_status(ret);
        for (auto volume : volumes)
        {
            res->add_volumes()->CopyFrom(volume);
        }
        return Status::OK;
    }
    else
    {
        res->set_status(ret);
        return Status::CANCELLED;
    }
}
