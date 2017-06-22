//
// Created by smile on 2017/5/17.
//

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>
#include <errno.h>
#include <regex.h>
#include <fstream>
#include <fcntl.h>
#include <string.h>
#include "common/env_posix.h"
#include "common/config_option.h"
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include "log/log.h"
#include "control_iscsi.h"

using namespace std;

// start class LunTuple
LunTuple::LunTuple(uint32_t tid, std::string iqn, uint32_t lun,
                   std::string volume_name, std::string block_device) {
    tid_ = tid;
    iqn_ = iqn;
    lun_ = lun;
    volume_name_ = volume_name;
    block_device_ = block_device;
}

LunTuple::LunTuple(const LunTuple& other) {
    tid_ = other.tid_;
    iqn_ = other.iqn_;
    lun_ = other.lun_;
    volume_name_  = other.volume_name_;
    block_device_ = other.block_device_;
}

LunTuple& LunTuple::operator=(const LunTuple& other) {
    if (this != &other) {
        tid_ = other.tid_;
        iqn_ = other.iqn_;
        lun_ = other.lun_;
        volume_name_ = other.volume_name_;
        block_device_ = other.block_device_;
    }
    return *this;
}

std::ostream& operator<<(std::ostream& cout, const LunTuple& lun) {
    LOG_INFO << "[tid:" << lun.tid_ << " iqn:" << lun.iqn_ \
             << " lun:" << lun.lun_ << " vol:" << lun.volume_name_  \
             << " bdev:" << lun.block_device_ + "]";
    return cout;
}
// end class LunTuple

// start class ISCSIControl
int ISCSIControl::tid_id = 100;
int ISCSIControl::lun_id = 200;

ISCSIControl::ISCSIControl(const std::string& host, const std::string& port):
    host_(host), port_(port)
{
    target_path_ = g_option.iscsi_target_config_dir;
    target_prefix_ = g_option.iscsi_target_prefix;
}

std::string ISCSIControl::get_target_iqn(const std::string& volume_id)
{
    return target_prefix_ + volume_id;
}

bool ISCSIControl::generate_config(const std::string& volume_id,
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

bool ISCSIControl::persist_config(const std::string& volume_id,
                                  const std::string& config)
{
    std::string file_name = target_path_ + volume_id;
    LOG_INFO<<"persist config file " << file_name;
    std::ofstream f(file_name);
    f << config;
    f.close();
    return true;
}

bool ISCSIControl::remove_config(const std::string& volume_id)
{
    std::string file_name = target_path_ + volume_id;
    LOG_INFO<<"remove config file "<<file_name;
    int ret = remove(file_name.c_str());
    if (ret == 0) {
        return true;
    } else {
        LOG_INFO<<"remove config file "<<file_name<<"failed";
        return false;
    }
}

bool ISCSIControl::add_target(const LunTuple& lun)
{
    LOG_INFO <<"add target " << lun;
    std::string cmd = "tgtadm --lld iscsi --mode target --op new --tid " \
                  + std::to_string(lun.tid_) + " --targetname " + lun.iqn_;
    LOG_INFO << cmd ;
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"add target "<< lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool ISCSIControl::remove_target(uint32_t tid)
{
    LOG_INFO<<"remove target tid:" << tid;
    std::string cmd = "tgtadm --lld iscsi --mode target --op delete --force --tid " \
                  + std::to_string(tid);
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"remove target tid:" << tid << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool ISCSIControl::add_lun(const LunTuple& lun)
{
    LOG_INFO <<"add lun: " << lun;

    char bsopt[1024] = "\0";
    snprintf(bsopt, sizeof(bsopt), "--bsopts \"host=%s;port=%d;volume=%s;device=%s\"",
             "127.0.0.1", 9999, lun.volume_name_.c_str(), lun.block_device_.c_str());
    std::string cmd = "tgtadm --lld iscsi --mode logicalunit --op new  --tid " \
                      + std::to_string(lun.tid_) + " --lun " + std::to_string(lun.lun_) \
                      + " --backing-store " + lun.block_device_   \
                      + " --bstype hijacker " \
                      + std::string(bsopt);
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"add lun:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool ISCSIControl::remove_lun(const LunTuple& lun)
{
    LOG_INFO<<"remove lun: " << lun;
    std::string cmd = "tgtadm --lld iscsi --mode logicalunit --op delete --tid " \
                       + std::to_string(lun.tid_) + " --lun " + std::to_string(lun.lun_);
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"remove lun:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool ISCSIControl::acl_bind(const LunTuple& lun)
{
    LOG_INFO<<"acl bind:" << lun;
    std::string cmd = "tgtadm --lld iscsi --mode target --op bind --tid " \
                       + std::to_string(lun.tid_) + " --initiator-addres ALL";
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"acl bind:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool ISCSIControl::acl_unbind(const LunTuple& lun)
{
    LOG_INFO<<"acl unbind:" << lun;
    std::string cmd = "tgtadm --lld iscsi --mode target --op unbind --tid " \
                       + std::to_string(lun.tid_) + " --initiator-addres ALL";
    LOG_INFO << cmd ;
    int ret = system(cmd.c_str());
    LOG_INFO<<"acl unbind:" << lun << " ret:" << ret;
    return ret == 0 ? true : false;
}

bool ISCSIControl::generate_connection(const std::string& vol_name,
                                       const std::string& device,
                                       std::string& iqn_name,
                                       bool recover)
{
    bool result;
    std::string error_msg;
    LOG_INFO << "generate connection vol:" << vol_name;
    do {
        if(!recover)
            iqn_name = get_target_iqn(vol_name);
        auto it = tgt_luns_.find(vol_name);
        if(it != tgt_luns_.end()){
            LOG_INFO << " target for this volume already exist";
            return true;
        }
        //add iscsi target
        LunTuple lun_tuple(tid_id++, iqn_name, lun_id++, vol_name, device);
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
            generate_config(vol_name, device, iqn_name, config);
            result = persist_config(vol_name, config);
            if (!result){
                error_msg = " failed persit config";
                break;
            }
        }
        tgt_luns_.insert({vol_name, lun_tuple});
        LOG_INFO << "generate connection vol:" << vol_name << " ok";
        return true;
    }while(0);

    LOG_INFO << "generate connection vol:" << vol_name << error_msg;
    return false;
}

bool ISCSIControl::initialize_connection(const std::string& vol_name,
                                         const std::string& device,
                                         std::map<std::string, std::string>& connection_info)
{
    std::string error_msg;
    std::string iqn_name;
    LOG_INFO << "initialize connection vol:" << vol_name;

    if(generate_connection(vol_name, device, iqn_name)){
        LOG_INFO << "initialize connection vol:" << vol_name << " ok";
        auto lun_tuple = tgt_luns_.find(vol_name);
        connection_info["target_iqn"] = iqn_name;
        connection_info["target_lun"] = std::to_string(lun_tuple->second.lun_);
        connection_info["display_name"] = vol_name;
        return true;
    }

    LOG_INFO << "initialize connection vol:" << vol_name << error_msg;
    return false;
}

bool ISCSIControl::terminate_connection(const std::string& vol_name)
{
    bool ret;
    std::string error_msg;
    LOG_INFO << "terminate connection vol:" << vol_name;
    do {
        auto it = tgt_luns_.find(vol_name);
        if (it == tgt_luns_.end()) {
            LOG_INFO << " target for this volume not exist";
            return true;
        }
        ret = acl_unbind(it->second);
        if (!ret) {
            error_msg = " failed acl unbind";
            break;
        }
        ret = remove_lun(it->second);
        if (!ret) {
            error_msg = " failed remove lun";
            break;
        }
        ret = remove_target(it->second.tid_);
        if (!ret) {
            error_msg = " failed remove target";
            break;
        }

        ret = remove_config(vol_name);
        if (!ret) {
            error_msg = " failed remove config";
            break;
        }
        tgt_luns_.erase(vol_name);
        LOG_INFO << "terminate connection vol:" << vol_name << " ok";
        return true;
    } while(0);

    LOG_INFO << "terminate connection vol:" << vol_name << error_msg;
    return false;
}

bool ISCSIControl::recover_targets()
{
    DIR* pdir = opendir(target_path_.c_str());
    if(pdir == nullptr){
        LOG_ERROR << "opendir path:" << target_path_ << "failed:" << errno;
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

bool ISCSIControl::recover_target(const char* vol_name)
{
    std::string vol_conf_path;
    vol_conf_path.append(target_path_);
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
    std::string line;
    std::string iqn_name;
    std::string dev_name;
    ifstream inif;
    inif.open(vol_conf_path.c_str());

    while(inif.good()){
        getline(inif, line);
        LOG_INFO << "read line:" << line;
        ret = regexec(&iqn_regex, line.c_str(), match_size, match_pos, 0);
        if(!ret){
            /*get iqn std::string*/
            LOG_INFO << "1 read iqn so:" << match_pos[0].rm_so << " eo:" << match_pos[0].rm_eo;
            off_t  match_start = match_pos[0].rm_so;
            size_t match_len = match_pos[0].rm_eo - match_pos[0].rm_so;
            iqn_name = line.substr(match_start, match_len);
        }
        ret = regexec(&dev_regex, line.c_str(), match_size, match_pos, 0);
        if(!ret){
            /*get dev std::string*/
            LOG_INFO << "2 read iqn so:" << match_pos[0].rm_so << " eo:" << match_pos[0].rm_eo;
            off_t  match_start = match_pos[0].rm_so;
            size_t match_len = match_pos[0].rm_eo - match_pos[0].rm_so;
            dev_name = line.substr(match_start, match_len);
        }
    }

    LOG_INFO << "iqn_name:" << iqn_name << " dev_name" << dev_name;
    if(!iqn_name.empty() && !dev_name.empty()){
        generate_connection(vol_name, dev_name, iqn_name, true);
    }

    regfree(&dev_regex);
    regfree(&iqn_regex);
    inif.close();
    return true;
}
//end class ISCSIControl
