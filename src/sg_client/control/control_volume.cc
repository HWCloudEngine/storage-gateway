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
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include "control_volume.h"
#include "common/block_dev.h"

using namespace std;
using google::protobuf::Map;
using huawei::proto::StatusCode;
using huawei::proto::VolumeInfo;
using huawei::proto::VOL_AVAILABLE;
using huawei::proto::VOL_ENABLING;

static const char* TGT_CONF_PATH = "/etc/tgt/conf.d";

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

bool VolumeControlImpl::recover_targets()
{
    DIR* pdir = opendir(TGT_CONF_PATH);
    if(pdir == nullptr){
        LOG_ERROR << "opendir path:" << TGT_CONF_PATH << "failed:" << errno;
        return false; 
    }
    struct dirent* pde;
    while((pde=readdir(pdir))){
        if(strcmp(pde->d_name, ".") == 0 || strcmp(pde->d_name, "..") == 0) 
            continue;
        LOG_INFO << "recover tgt volume:" << pde->d_name; 
        bool ret = recover_target(pde->d_name);
        LOG_INFO << "recover tgt volume:" << pde->d_name << " ret:" << ret; 
    }

    closedir(pdir);
    return true;
}

bool VolumeControlImpl::recover_target(const char* vol_name)
{
    string vol_conf_path;
    vol_conf_path.append(TGT_CONF_PATH);
    vol_conf_path.append("/");
    vol_conf_path.append(vol_name);

    LOG_INFO << "read tgt volume conf path:" << vol_conf_path; 

    regex_t iqn_regex;
    regex_t dev_regex;
    int ret = regcomp(&iqn_regex, "(iqn).([0-9]+)-([0-9]+).([a-z]+).([a-z]+).([a-z0-9-]+)", REG_EXTENDED);
    ret |= regcomp(&dev_regex, "/dev/[a-z]+", REG_EXTENDED);
    if(ret){
        LOG_ERROR << "regcomp failed:" << errno;
        return false;
    }
    
    const int match_size = 10;
    regmatch_t match_pos[match_size];
    string line;
    string iqn_name;
    string dev_name;
    ifstream inif;
    inif.open(vol_conf_path.c_str());

    while(inif.good()){
        getline(inif, line);
        LOG_INFO << "read line:" << line;
        ret = regexec(&iqn_regex, line.c_str(), match_size, match_pos, 0);
        if(!ret){
            /*get iqn string*/
            LOG_INFO << "1 read iqn so:" << match_pos[0].rm_so << " eo:" << match_pos[0].rm_eo;
            off_t  match_start = match_pos[0].rm_so;
            size_t match_len = match_pos[0].rm_eo - match_pos[0].rm_so;
            iqn_name = line.substr(match_start, match_len);
        }
        ret = regexec(&dev_regex, line.c_str(), match_size, match_pos, 0);
        if(!ret){
            /*get dev string*/
            LOG_INFO << "2 read iqn so:" << match_pos[0].rm_so << " eo:" << match_pos[0].rm_eo;
            off_t  match_start = match_pos[0].rm_so;
            size_t match_len = match_pos[0].rm_eo - match_pos[0].rm_so;
            dev_name = line.substr(match_start, match_len);
        }
    }
    
    LOG_INFO << "iqn_name:" << iqn_name << " dev_name" << dev_name;
    if(!iqn_name.empty() && !dev_name.empty()){
        BlockDevice blk(dev_name);
        size_t dev_size = blk.dev_size();
        enable_sg(vol_name, dev_name, dev_size, iqn_name, true);
    }

    regfree(&dev_regex);
    regfree(&iqn_regex);
    inif.close();
    return true;
}

bool VolumeControlImpl::enable_sg(const string vol_name, 
                                  const string dev_name, 
                                  const size_t dev_size,
                                  string& iqn_name, 
                                  bool recover)
{
    bool result = false;
    string error_msg;

    do {
        auto it = tgt_luns_.find(vol_name);
        if(it != tgt_luns_.end()){
            error_msg = " failed sg has enabled already";
            break;
        }
        //add iscsi target
        if(iqn_name.empty()){
            iqn_name = get_target_iqn(vol_name);
        }
        LunTuple lun_tuple(tid_id++, iqn_name, lun_id++, vol_name, dev_name);
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
       //persist config and update target
        if(!recover){
            std::string config;
            generate_config(vol_name, dev_name, iqn_name, config);
            result = persist_config(vol_name, config);
            if (!result){
                error_msg = " failed persit config" ;
                break;
            }
        }
        tgt_luns_.insert({vol_name, lun_tuple});
        LOG_INFO << "enable sg vol:" << vol_name << " ok";
        return true;
    }while(0);

    LOG_INFO << "enable sg vol:" << vol_name << error_msg;
    return false;
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
    }
    return Status::OK;
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
    string vol_name = req->volume_id();
    string dev_name = req->device();
    size_t dev_size = req->size();
    string iqn_name;
    bool result;

    LOG_INFO << "enable sg vol:" << vol_name << " device:" << dev_name;
    StatusCode ret = vol_inner_client_->create_volume(vol_name, dev_name, dev_size, VOL_AVAILABLE);
    if(ret != StatusCode::sOk){
        res->set_status(StatusCode::sInternalError);
        LOG_ERROR << "enable sg vol:" << vol_name << " device:" << dev_name << " failed"; 
        return Status::OK;
    }

    result = enable_sg(vol_name, dev_name, dev_size, iqn_name, false);
    if(!result){
        ret = vol_inner_client_->delete_volume(vol_name);
        res->set_status(StatusCode::sInternalError);
        LOG_ERROR << "enable sg vol:" << vol_name << " device:" << dev_name << " failed"; 
        return Status::OK;
    }

    (*res->mutable_driver_data())["driver_type"] = "iscsi";
    (*res->mutable_driver_data())["target_iqn"] = iqn_name;
    res->set_status(StatusCode::sOk);
    LOG_ERROR << "enable sg vol:" << vol_name << " device:" << dev_name << " ok"; 
    return Status::OK;
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
    return Status::OK;
}

Status VolumeControlImpl::GetVolume(ServerContext* context,
        const control::GetVolumeReq* req, control::GetVolumeRes* res)
{
    std::string volume_id = req->volume_id();
    VolumeInfo volume;
    StatusCode ret = vol_inner_client_->get_volume(volume_id, volume);
    if (ret == StatusCode::sOk)
    {
        res->mutable_volume()->CopyFrom(volume);
    }
    res->set_status(ret);
    return Status::OK;
}

Status VolumeControlImpl::ListVolumes(ServerContext* context,
        const control::ListVolumesReq* req, control::ListVolumesRes* res)
{
    std::list<VolumeInfo> volumes;
    StatusCode ret = vol_inner_client_->list_volume(volumes);
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
