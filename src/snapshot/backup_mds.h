#ifndef _BACKUP_MDS_H
#define _BACKUP_MDS_H

#include "abstract_mds.h"
#include "backup_type.h"
#include "block_store.h"
#include "index_store.h"
#include "../rpc/clients/snapshot_ctrl_client.h"

class BackupMds : public AbstractMds
{
public:
    BackupMds(const string& vol_name, const size_t& vol_size, AbstractMds* snap_mds);
    virtual ~BackupMds();   

    StatusCode sync(const SyncReq* req, SyncAck* ack) override;

    /*snapshot common operation*/
    StatusCode create_snapshot(const CreateReq* req, CreateAck* ack) override;
    StatusCode delete_snapshot(const DeleteReq* req, DeleteAck* ack) override;
    StatusCode rollback_snapshot(const RollbackReq* req, RollbackAck* ack) override;
    StatusCode list_snapshot(const ListReq* req, ListAck* ack) override;
    StatusCode query_snapshot(const QueryReq* req, QueryAck* ack) override;
    StatusCode diff_snapshot(const DiffReq* req, DiffAck* ack) override;    
    StatusCode read_snapshot(const ReadReq* req, ReadAck* ack) override;
    
    /*snapshot status*/
    StatusCode update(const UpdateReq* req, UpdateAck* ack) override;
    
    /*cow*/
    StatusCode cow_op(const CowReq* req, CowAck* ack) override;
    StatusCode cow_update(const CowUpdateReq* req, CowUpdateAck* ack) override;
    
    /*crash recover*/
    int recover() override;

private:
    StatusCode async_create_backup();
    StatusCode async_delete_backup();

    StatusCode full_backup();
    StatusCode incr_backup();

    backupid_t get_backup_id(string backup_name);
    string     get_backup_name(backupid_t backup_id);
    
    backupid_t spawn_backup_id();

    string spawn_backup_object_name(const backupid_t backup_id, 
                                    const block_t    blk_id);

    void split_backup_object_name(const string& raw, 
                                  string& vol_name,
                                  backupid_t& backup_id, 
                                  block_t&  blk_id);
    /*debug*/
    void trace();

private:
    /*volume basic*/
    string m_vol_name;
    size_t m_vol_size;
    /*lock*/
    mutex m_mutex;
    /*store all snapshot mds*/
    AbstractMds* m_snap_mds;

    /*backup basic*/
    string     m_pre_backup;
    string     m_cur_backup;

    backupid_t m_latest_backup_id;
    string     m_latest_full_backup;

    /*backup and attr map*/
    map<string, backup_attr_t> m_backups;
    /*backup id and backup block map*/
    map<backupid_t, map<block_t, backup_object_t>> m_backup_block_map;
    /*backup object and backup referece map*/
    map<backup_object_t, backup_object_ref_t> m_backup_object_map;

    /*index store for backup meta*/
    IndexStore* m_index_store;
    /*block store for backup data*/
    BlockStore* m_block_store;

    /*snapshot client for reading incremental data and metadata */
    SnapCtrlClient* m_snap_client;
};

#endif
