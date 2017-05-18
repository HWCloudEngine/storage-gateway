//
// Created by smile on 2017/5/17.
//

#ifndef _CONTROL_ISCSI_H
#define _CONTROL_ISCSI_H

#include <iostream>
#include <list>
#include <map>
#include <string>

class LunTuple {
public:
    LunTuple(uint32_t tid, std::string iqn, uint32_t lun,
             std::string volume_name, std::string block_device);
    LunTuple(const LunTuple& other);
    LunTuple& operator=(const LunTuple& other);
    ~LunTuple() {
    }

    friend std::ostream& operator<<(std::ostream& cout, const LunTuple& lun);

public:
    uint32_t tid_;
    std::string   iqn_;
    uint32_t lun_;
    std::string   volume_name_;
    std::string   block_device_;
};

class ISCSIControl
{
public:
    ISCSIControl(const std::string& host, const std::string& port);
    bool initialize_connection(const std::string& vol_name,
                               const std::string& device,
                               std::map<std::string, std::string>& connection_info);
    bool terminate_connection(const std::string& vol_name);
    bool recover_targets();
private:
    std::string get_target_iqn(const std::string& volume_id);
    bool generate_connection(const std::string& vol_name,
                             const std::string& device,
                             std::string& iqn_name,
                             bool recover = false);
    bool generate_config(const std::string& volume_id,
                         const std::string& device,
                         const std::string& target_iqn,
                         std::string& config);
    bool persist_config(const std::string& volume_id,
                        const std::string& config);
    bool remove_config(const std::string& volume_id);

    bool add_target(const LunTuple& lun);
    bool remove_target(uint32_t tid);
    bool add_lun(const LunTuple& lun);
    bool remove_lun(const LunTuple& lun);
    bool acl_bind(const LunTuple& lun);
    bool acl_unbind(const LunTuple& lun);

    /*recover targets*/
    bool recover_target(const char* vol_name);

    std::string host_;
    std::string port_;
    std::string target_path_;
    std::string target_prefix_;
    static int tid_id;
    static int lun_id;
    std::map<std::string, LunTuple> tgt_luns_;
};

#endif //_CONTROL_ISCSI_H
