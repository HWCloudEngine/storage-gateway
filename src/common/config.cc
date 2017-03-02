#include "config_parser.h"
#include "config.h"


void Configure::init(const string& conf_file)
{
    ConfigParser config_parser(conf_file.c_str());
    
    const string default_iscsi_target_prefix = "iqn.2017-01.huawei.sgs.";
    const string default_iscsi_target_config_dir = "/etc/tgt/conf.d/";
    iscsi_target_prefix = config_parser.get_default("iscsi.target_prefix", default_iscsi_target_prefix);
    iscsi_target_config_dir = config_parser.get_default("iscsi.target_config_dir", default_iscsi_target_config_dir);
    
    ctrl_server_ip   = config_parser.get_default("ctrl_server.ip", string("127.0.0.1"));
    ctrl_server_port = config_parser.get_default("ctrl_server.port", 1111);
    
    global_journal_meta_storage = config_parser.get_default("global.journal_meta_storage", string("ceph_s3"));
    global_journal_data_storage = config_parser.get_default("global.journal_data_storage", string("ceph_fs"));

    ceph_s3_access_key = config_parser.get_default("ceph_s3.access_key", string(""));
    ceph_s3_secret_key = config_parser.get_default("ceph_s3.secret_key", string(""));
    ceph_s3_host       = config_parser.get_default("ceph_s3.host", string(""));
    ceph_s3_bucket     = config_parser.get_default("ceph_s3.bucket", string(""));

    gc_window = config_parser.get_default("ceph_s3.gc_window",100);

    lease_renew_window    = config_parser.get_default("ceph_s3.lease_renew_window",100);
    lease_expire_window   = config_parser.get_default("ceph_s3.lease_expire_window",600);
    lease_validity_window = config_parser.get_default("ceph_s3.lease_validity_window",150);

    meta_server_ip   = config_parser.get_default("meta_server.ip", string("127.0.0.1"));
    meta_server_port = config_parser.get_default("meta_server.port", 50051);
   
    journal_interval = config_parser.get_default("ceph_s3.get_journal_interval",500);
    journal_limit    = config_parser.get_default("ceph_s3.journal_limit",4);
    journal_max_size      = config_parser.get_default("journal.max_size",32 * 1024 * 1024);
    journal_mount_point   = config_parser.get_default("journal.mount_point", string("/mnt/cephfs"));
    journal_write_timeout = config_parser.get_default("journal.write_timeout",2);
    journal_process_thread_num = config_parser.get_default("journal.process_thread_num",2);

    replicate_local_ip   = config_parser.get_default("replicate.local_ip", string("127.0.0.1"));
    replicate_remote_ip  = config_parser.get_default("replicate.remote_ip", string("127.0.0.1"));
    replicate_port       = config_parser.get_default("replicate.port", 50061);
}

string Configure::sg_server_addr()const
{
    return meta_server_ip + ":" + to_string(meta_server_port);
}
