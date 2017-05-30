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
#include "config_parser.h"
#include "config_option.h"

#define DEFAULT_CONFIG_FILE "/etc/storage-gateway/config.ini"

ConfigureOptions::ConfigureOptions() {
    ConfigParser config_parser(DEFAULT_CONFIG_FILE);
    const std::string default_iscsi_target_prefix = "iqn.2017-01.huawei.sgs.";
    const std::string default_iscsi_target_config_dir = "/etc/tgt/conf.d/";

    iscsi_target_prefix = config_parser.get_default("iscsi.target_prefix", default_iscsi_target_prefix);
    iscsi_target_config_dir = config_parser.get_default("iscsi.target_config_dir", default_iscsi_target_config_dir);
    agent_dev_conf = config_parser.get_default("agent.dev_conf", std::string("/etc/storage-gateway/agent_dev.conf"));
    volumes_conf = config_parser.get_default("volumes.volumes_conf", std::string("/etc/storage-gateway/volumes.conf"));

    ctrl_server_ip = config_parser.get_default("ctrl_server.ip", std::string("127.0.0.1"));
    ctrl_server_port = config_parser.get_default("ctrl_server.port", 1111);

    global_max_volume_count = config_parser.get_default("global.max_volume_count",128);
    global_journal_meta_storage = config_parser.get_default("global.journal_meta_storage", std::string("ceph_s3"));
    global_journal_data_storage = config_parser.get_default("global.journal_data_storage", std::string("ceph_fs"));
    global_client_mode = config_parser.get_default("global.client_mode",0);

    ceph_s3_access_key = config_parser.get_default("ceph_s3.access_key", std::string(""));
    ceph_s3_secret_key = config_parser.get_default("ceph_s3.secret_key", std::string(""));
    ceph_s3_host = config_parser.get_default("ceph_s3.host", std::string(""));
    ceph_s3_bucket = config_parser.get_default("ceph_s3.bucket", std::string(""));
    ceph_cluster_name = config_parser.get_default("ceph_cluster_name", std::string("ceph"));
    ceph_user_name = config_parser.get_default("ceph_user_name", std::string("client.admin"));
    ceph_pool_name = config_parser.get_default("ceph_pool_name", std::string("mypool"));

    gc_window = config_parser.get_default("ceph_s3.gc_window",100);

    lease_renew_window = config_parser.get_default("ceph_s3.lease_renew_window",100);
    lease_expire_window = config_parser.get_default("ceph_s3.lease_expire_window",600);
    lease_validity_window = config_parser.get_default("ceph_s3.lease_validity_window",150);

    meta_server_ip = config_parser.get_default("meta_server.ip", std::string("127.0.0.1"));
    meta_server_port = config_parser.get_default("meta_server.port", 50051);

    journal_interval = config_parser.get_default("ceph_s3.get_journal_interval",500);
    journal_limit = config_parser.get_default("ceph_s3.journal_limit",4);
    journal_max_size = config_parser.get_default("journal.max_size",32 * 1024 * 1024);
    journal_mount_point = config_parser.get_default("journal.mount_point", std::string("/mnt/cephfs"));
    journal_write_timeout = config_parser.get_default("journal.write_timeout",2);
    journal_process_thread_num = config_parser.get_default("journal.process_thread_num",2);
    journal_producer_marker_update_interval = config_parser.get_default( \
                "journal_writer.producer_marker_update_interval",5000);
    journal_producer_written_size_threshold = config_parser.get_default( \
                "journal_writer.producer_written_size_threshold",4*1024*1024);

    replicate_local_ip = config_parser.get_default("replicate.local_ip", std::string("127.0.0.1"));
    replicate_remote_ip  = config_parser.get_default("replicate.remote_ip", std::string("127.0.0.1"));
    replicate_port = config_parser.get_default("replicate.port", 50061);
}

ConfigureOptions::~ConfigureOptions() {
}

ConfigureOptions& ConfigureOptions::instance() {
    static ConfigureOptions opts;
    return opts;
}

