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
    /*general*/
    int global_client_mode;
    std::string global_storage_media;
    /*lease*/
    int lease_renew_window;
    int lease_expire_window;
    int lease_validity_window;
    /*ceph*/
    std::string ceph_s3_access_key;
    std::string ceph_s3_secret_key;
    std::string ceph_s3_bucket;
    std::string ceph_host;
    std::string ceph_cluster_name;
    std::string ceph_user_name;
    std::string ceph_pool_name;
    /*local*/
    std::string local_agent_dev_conf;
    std::string local_volumes_conf;
    std::string local_meta_path;
    std::string local_data_path;
   /*journal*/
    int journal_interval;
    int journal_limit;
    int journal_max_size;
    int journal_write_timeout;
    std::string journal_mount_point;
    int journal_process_thread_num;
    int journal_producer_marker_update_interval;
    int journal_producer_written_size_threshold;
    int journal_gc_window;

    /*ctrl server*/
    std::string io_server_ip;
    int io_server_port;
    std::string io_server_uds;
    std::string ctrl_server_ip;
    int ctrl_server_port;
    std::string meta_server_ip;
    int meta_server_port;
    std::string replicate_local_ip;
    std::string replicate_remote_ip;
    int replicate_port;
    int replicate_frame_size;
};

#define g_option (ConfigureOptions::instance())

#endif  //  SRC_COMMON_CONFIG_OPTION_H_
