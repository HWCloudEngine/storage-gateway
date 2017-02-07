#ifndef _BACKUP_MDS_H
#define _BACKUP_MDS_H
#include "../rpc/backup_inner_control.pb.h"
#include "../rpc/backup_inner_control.grpc.pb.h"
#include "../rpc/clients/snapshot_ctrl_client.h"
#include "../common/block_store.h"
#include "../common/index_store.h"
#include "../snapshot/snapshot_type.h"
#include "backup_type.h"

using huawei::proto::inner::CreateBackupInReq;
using huawei::proto::inner::CreateBackupInAck;
using huawei::proto::inner::ListBackupInReq;
using huawei::proto::inner::ListBackupInAck;
using huawei::proto::inner::GetBackupInReq;
using huawei::proto::inner::GetBackupInAck;
using huawei::proto::inner::DeleteBackupInReq;
using huawei::proto::inner::DeleteBackupInAck;
using huawei::proto::inner::RestoreBackupInReq;
using huawei::proto::inner::RestoreBackupInAck;

class BackupMds 
{
public:
    BackupMds(const string& vol_name, const size_t& vol_size);
    virtual ~BackupMds();   

    /*snapshot common operation*/
    StatusCode create_backup(const CreateBackupInReq* req, CreateBackupInAck* ack);
    StatusCode delete_backup(const DeleteBackupInReq* req, DeleteBackupInAck* ack);
    StatusCode restore_backup(const RestoreBackupInReq* req, RestoreBackupInAck* ack);
    StatusCode list_backup(const ListBackupInReq* req, ListBackupInAck* ack);
    StatusCode get_backup(const GetBackupInReq* req, GetBackupInAck* ack);
    
    /*crash recover*/
    int recover();

private:
    StatusCode async_create_backup();
    StatusCode async_delete_backup(const string& backup_name);
    
    /*create backup*/
    StatusCode do_full_backup();
    StatusCode do_incr_backup();
    
    /*delete backup*/
    StatusCode do_delete_backup(const string& backup_name);
    StatusCode do_merge_backup(const string& cur_backup, const string& next_backup);

    bool is_backup_exist(const string& backup_name);
    bool is_incr_backup_allowable();
    bool is_backup_deletable(const string& backup_name);

    backupid_t get_backup_id(const string& backup_name);
    string     get_backup_name(const backupid_t& backup_id);
    BackupMode get_backup_mode(const string& backup_name);
    BackupStatus get_backup_status(const string& backup_name);
    
    /*get the latest full backup in system*/
    string get_latest_full_backup();
    /*give a backup, get the base backup of the backup*/
    string get_backup_base(const string& cur_backup);

    /*according current backup name to get prev/next backup name*/
    string get_prev_backup(const string& cur_backup);
    string get_next_backup(const string& cur_backup);

    backupid_t spawn_backup_id();

    string spawn_backup_object_name(const backupid_t& backup_id, 
                                    const block_t&    blk_id);
    void split_backup_object_name(const string& raw, 
                                  const string& vol_name,
                                  const backupid_t& backup_id, 
                                  const block_t&  blk_id);
    /*debug*/
    void trace();

private:
    /*volume basic*/
    string m_vol_name;
    size_t m_vol_size;
    /*lock*/
    recursive_mutex m_mutex;

    /*backup basic*/
    backupid_t m_latest_backup_id;
    string m_cur_backup;

    /*backup and attr map*/
    map<string, backup_attr_t> m_backups;
    /*backup id and backup block map*/
    map<backupid_t, map<block_t, backup_object_t>> m_backup_block_map;

    /*index store for backup meta*/
    IndexStore* m_index_store;
    /*block store for backup data*/
    BlockStore* m_block_store;

    /*snapshot client for reading incremental data and metadata */
    SnapshotCtrlClient* m_snap_client;
};

#endif
