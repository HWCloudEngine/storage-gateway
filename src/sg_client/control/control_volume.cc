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
#include <assert.h>
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include "control_volume.h"

using google::protobuf::Map;
using huawei::proto::StatusCode;
using huawei::proto::VolumeInfo;
using huawei::proto::VOL_AVAILABLE;
using huawei::proto::VOL_ENABLING;

LunTuple::LunTuple(uint32_t tid, string iqn, uint32_t lun, 
                   string volume_name, string block_device)
{
    tid_ = tid; 
    iqn_ = iqn;
    lun_ = lun;
    volume_name_  = volume_name;
    block_device_ = block_device;
}

LunTuple::LunTuple(const LunTuple& other)
{
    tid_ = other.tid_; 
    iqn_ = other.iqn_;
    lun_ = other.lun_;
    volume_name_  = other.volume_name_;
    block_device_ = other.block_device_;
}

LunTuple& LunTuple::operator=(const LunTuple& other)
{
    if(this != &other){
        tid_ = other.tid_; 
        iqn_ = other.iqn_;
        lun_ = other.lun_;
        volume_name_  = other.volume_name_;
        block_device_ = other.block_device_;
    }
    return *this;    
}

ostream& operator<<(ostream& cout, const LunTuple& lun)
{
    LOG_INFO << "[tid:" << lun.tid_ << " iqn:" << lun.iqn_ \
             << " lun:" << lun.lun_ << " vol:" << lun.volume_name_  \
             << " bdev:" << lun.block_device_ + "]";
    return cout;
}


int VolumeControlImpl::tid_id = 100;
int VolumeControlImpl::lun_id = 200;

VolumeControlImpl::VolumeControlImpl(const Configure& conf, const std::string& host,
                                     const std::string& port,
                                     std::shared_ptr<VolInnerCtrlClient> vol_inner_client) 
    : conf_(conf), host_(host), port_(port), vol_inner_client_(vol_inner_client)
{
    target_prefix_ = conf_.iscsi_target_prefix;
    target_path_  = conf_.iscsi_target_config_dir;
}

bool VolumeControlImpl::create_volume(const std::string& volume_id, size_t size,
                                      const std::string& device)
{
    LOG_INFO << "create volume vname:" << volume_id << "size:" << size;
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
    LOG_INFO << "delete volume vname:" << volume_id;
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
                                             const VolumeStatus& status)
{
    LOG_INFO << "update volume vname:" << volume_id << " status:" << status;
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

std::string VolumeControlImpl::get_target_iqn(const std::string& volume_id)
{
    return target_prefix_ + volume_id;
}

bool VolumeControlImpl::generate_config(const std::string& volume_id,
                                        const std::string& device, 
                                        const std::string& target_iqn,
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
    LOG_INFO<<"persist config file " << file_name;
    std::ofstream f(file_name);
    f << config;
    f.close();
    return true;
}

bool VolumeControlImpl::execute_cmd(const std::string& command, std::string& result)
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
    LOG_INFO<<"update target " << target_iqn;
    std::string cmd = "tgt-admin --update " + target_iqn + " -f";
    int ret = system(cmd.c_str());
    if (ret == 0)
    {
        return true;
    }
    else
    {
        LOG_INFO<<"update target " << target_iqn << " failed errno:" << errno;
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
    int ret = system(cmd.c_str());
    if(ret == -1){
        LOG_ERROR << "blockdev flushbufs failed.";
        return false;
    }

    std::string path = "/sys/block/" + device.substr(5) + "/device/delete";
    cmd = "echo 1 | tee -a " + path;
    ret = system(cmd.c_str());
    if(ret == -1){
        LOG_ERROR << "blockdev delete failed.";
        return false;
    }

    return true;
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
        return Status::CANCELLED;
    }

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

bool VolumeControlImpl::add_target(const LunTuple& lun)
{
    LOG_INFO <<"add target " << lun;
    string cmd = "tgtadm --lld iscsi --mode target --op new --tid " \
                  + to_string(lun.tid_) + " --targetname " + lun.iqn_;
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"add target "<< lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool VolumeControlImpl::remove_target(uint32_t tid)
{
    LOG_INFO<<"remove target tid:" << tid;
    string cmd = "tgtadm --lld iscsi --mode target --op delete --force --tid " \
                  + to_string(tid);
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"remove target tid:" << tid << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool VolumeControlImpl::add_lun(const LunTuple& lun)
{
    LOG_INFO <<"add lun: " << lun;

    char bsopt[1024] = "\0";
    snprintf(bsopt, sizeof(bsopt), "--bsopts \"host=%s;port=%d;volume=%s;device=%s\"",
            "127.0.0.1", 9999, lun.volume_name_.c_str(), lun.block_device_.c_str());
    std::string cmd = "tgtadm --lld iscsi --mode logicalunit --op new  --tid " \
                      + to_string(lun.tid_) + " --lun " + to_string(lun.lun_) \
                      + " --backing-store " + lun.block_device_   \
                      + " --bstype hijacker " \
                      + string(bsopt);
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"add lun:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool VolumeControlImpl::remove_lun(const LunTuple& lun)
{    
    LOG_INFO<<"remove lun: " << lun;
    std::string cmd = "tgtadm --lld iscsi --mode logicalunit --op delete --tid " \
                       + to_string(lun.tid_) + " --lun " + to_string(lun.lun_);
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"remove lun:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool VolumeControlImpl::acl_bind(const LunTuple& lun)
{
    LOG_INFO<<"acl bind:" << lun;
    std::string cmd = "tgtadm --lld iscsi --mode target --op bind --tid " \
                       + to_string(lun.tid_) + " --initiator-addres ALL";
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"acl bind:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool VolumeControlImpl::acl_unbind(const LunTuple& lun)
{
    LOG_INFO<<"acl unbind:" << lun;
    std::string cmd = "tgtadm --lld iscsi --mode target --op unbind --tid " \
                       + to_string(lun.tid_) + " --initiator-addres ALL";
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"acl unbind:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

Status VolumeControlImpl::EnableSG(ServerContext* context,
                                   const control::EnableSGReq* req, 
                                   control::EnableSGRes* res)
{
    std::string volume_id = req->volume_id();
    std::string device = req->device();
    size_t size = req->size();
    std::string error_msg;
    bool result;

    LOG_INFO << "enable sg vol:" << volume_id << " device:" << device;
   
    do {
        auto it = tgt_luns_.find(volume_id);
        if(it != tgt_luns_.end()){
            error_msg = " failed sg has enabled already";
            break;
        }
        //create volume on sg server
        result = create_volume(volume_id, size, device);
        if(!result){
            error_msg = " failed create volume on sg server";
            break;
        }
        //add iscsi target
        std::string target_iqn;
        target_iqn = get_target_iqn(volume_id);
        LunTuple lun_tuple(tid_id++, target_iqn, lun_id++, volume_id, device);
        result = add_target(lun_tuple);
        if(!result){
            error_msg = " failed add target";
            break;
        }
        //add iscsi lun
        result = add_lun(lun_tuple);
        if(!result){
            error_msg = " failed add lun";
            break;
        }
        result = acl_bind(lun_tuple);
        if(!result){
            error_msg = " failed acl_bind";
            break;
        }
        //update status on sg server
        result = update_volume_status(volume_id, VOL_AVAILABLE);
        if (!result){
            error_msg = " failed update volume status ";
            break;
        }
        //persist config and update target
        std::string config;
        generate_config(volume_id, device, target_iqn, config);
        result = persist_config(volume_id, config);
        if (!result){
            error_msg = " failed persit config" ;
            break;
        }
        tgt_luns_.insert({volume_id, lun_tuple});
        (*res->mutable_driver_data())["driver_type"] = "iscsi";
        (*res->mutable_driver_data())["target_iqn"] = target_iqn;
        res->set_status(StatusCode::sOk);
        LOG_ERROR << "enable sg vol:" << volume_id << " device:" << device << " ok"; 
        return Status::OK;
    }while(0);

    res->set_status(StatusCode::sInternalError);
    LOG_ERROR << "enable sg vol:" << volume_id << " device:" << device << error_msg; 
    return Status::CANCELLED;
}

Status VolumeControlImpl::DisableSG(ServerContext* context,
                                    const control::DisableSGReq* req, 
                                    control::DisableSGRes* res)
{
    std::string volume_id = req->volume_id();
    std::string target_iqn = get_target_iqn(volume_id);
    string error_msg;
    bool ret;
    LOG_INFO << "disable sg volume:" << volume_id;
   
    do {
        auto it = tgt_luns_.find(volume_id);
        if(it == tgt_luns_.end()){
            error_msg = " volume not exist";
            break;
        }
        ret = acl_unbind(it->second);
        if(!ret){
            error_msg = " failed acl unbind";
            break;
        }
        ret = remove_lun(it->second);
        if(!ret){
            error_msg = " failed remove lun";
            break;
        }
        ret = remove_target(it->second.tid_);
        if(!ret){
            error_msg = " failed remove target";
            break;
        }
        //ret = remove_device(it->second.block_device_);
        //if(!ret){
        //    error_msg = " failed remove device";
        //    break;
        //}

        ret = remove_config(volume_id);
        if(!ret){
            error_msg = " failed remove config";
            break;
        }
        tgt_luns_.erase(volume_id);
        LOG_INFO << "disable sg volume:" << volume_id << " ok";
        res->set_status(StatusCode::sOk);
        return Status::OK;
    } while(0);

    LOG_INFO << "disable sg volume:" << volume_id << error_msg;
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
