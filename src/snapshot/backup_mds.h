#ifndef _BACKUP_MDS_H
#define _BACKUP_MDS_H

#include "../rpc/clients/snapshot_ctrl_client.h"
#include "abstract_mds.h"
#include "snapshot_mds.h"
#include "backup_type.h"

class BackupMds : public AbstractMds
{
public:
    BackupMds(SnapshotMds* snap_mds);
    virtual ~BackupMds();   

    /*snapshot common operation*/
    StatusCode create_snapshot(const CreateReq* req, CreateAck* ack) override;
    StatusCode delete_snapshot(const DeleteReq* req, DeleteAck* ack) override;
    StatusCode rollback_snapshot(const RollbackReq* req, RollbackAck* ack) override;
    StatusCode list_snapshot(const ListReq* req, ListAck* ack) override;
    StatusCode query_snapshot(const QueryReq* req, QueryAck* ack) override;
    StatusCode diff_snapshot(const DiffReq* req, DiffAck* ack) override;    
    StatusCode read_snapshot(const ReadReq* req, ReadAck* ack) override;
    
    /*snapshot status*/
    virtual StatusCode update(const UpdateReq* req, UpdateAck* ack) override;
    
    /*cow*/
    virtual StatusCode cow_op(const CowReq* req, CowAck* ack) override;
    virtual StatusCode cow_update(const CowUpdateReq* req, CowUpdateAck* ack) override;
    
    /*crash recover*/
    int recover() override;

private:
    /*debug*/
    void trace();

private:
    /*volume basic*/
    string m_vol_name;
    size_t m_vol_size;

    /*store all snapshot mds*/
    SnapshotMds* m_snap_mds;

    /*backup basic*/
    string     m_pre_backup;
    string     m_cur_backup;
    backupid_t m_cur_backup_id;
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

    /*snapshot client for snapshot data and diff metadata */
    SnapCtrlClient* m_snap_client;
};

#endif
