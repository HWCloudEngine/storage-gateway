/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   config.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: all system configure parameters
*
*************************************************/
#ifndef SRC_COMMON_CONFIG_OPTION_H_
#define SRC_COMMON_CONFIG_OPTION_H_
#include <string>

class ConfigureOptions {
 private:
    ConfigureOptions();
    ~ConfigureOptions();
    
 public:
    static ConfigureOptions& instance();

    /*iscsi target*/
    std::string iscsi_target_prefix;
    std::string iscsi_target_config_dir;
    /*ctrl server*/
    std::string ctrl_server_ip;
    int ctrl_server_port;
    /*global*/
    int global_max_volume_count;
    std::string global_journal_meta_storage;
    std::string global_journal_data_storage;
    /* 0:tgt,1:agent */
    int global_client_mode;
    /*ceph s3*/
    std::string ceph_s3_access_key;
    std::string ceph_s3_secret_key;
    std::string ceph_s3_host;
    std::string ceph_s3_bucket;
    /*snapshot backup storage*/
    std::string ceph_cluster_name;
    std::string ceph_user_name;
    std::string ceph_pool_name;
    /*lease*/
    int lease_renew_window;
    int lease_expire_window;
    int lease_validity_window;
    /*journal*/
    int journal_interval;
    int journal_limit;
    int journal_max_size;
    int journal_write_timeout;
    std::string journal_mount_point;
    int journal_process_thread_num;
    int journal_producer_marker_update_interval;
    int journal_producer_written_size_threshold;
    /*sg server*/
    std::string meta_server_ip;
    int    meta_server_port;
    /*gc window*/
    int gc_window;
    /*replicate*/
    std::string replicate_local_ip;
    std::string replicate_remote_ip;
    int    replicate_port;
    /*agent*/
    std::string agent_dev_conf;
    /*volumes*/
    std::string volumes_conf;
};

#define g_option (ConfigureOptions::instance())

#endif  //  SRC_COMMON_CONFIG_OPTION_H_
