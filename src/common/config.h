#ifndef _CONFIG_H
#define _CONFIG_H
#include <string>
using namespace std;

#define DEFAULT_CONFIG_FILE "/etc/storage-gateway/config.ini"

class Configure 
{
public:
    Configure() = default;
    ~Configure(){};

    void init(const string& conf_file);
    
    string sg_server_addr()const;

public:
    /*iscsi target*/
    string iscsi_target_prefix;
    string iscsi_target_config_dir;
    
    /*ctrl server*/
    string ctrl_server_ip;
    int    ctrl_server_port;
    
    /*global*/
    int    global_max_volume_count;
    string global_journal_meta_storage;
    string global_journal_data_storage;

    /*ceph s3*/
    string ceph_s3_access_key;
    string ceph_s3_secret_key;
    string ceph_s3_host;     
    string ceph_s3_bucket;
    
    /*lease*/
    int lease_renew_window;
    int lease_expire_window;
    int lease_validity_window;
    
    /*journal*/
    int journal_interval;
    int journal_limit;
    int journal_max_size;
    int journal_write_timeout;
    string journal_mount_point;
    int journal_process_thread_num;
    int journal_producer_marker_update_interval;
    int journal_producer_written_size_threshold;

    /*sg server*/
    string meta_server_ip;
    int    meta_server_port;
    
    /*gc window*/
    int gc_window;

    /*replicate*/
    string replicate_local_ip;
    string replicate_remote_ip;
    int    replicate_port;
};

#endif
