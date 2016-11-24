#ifndef _SNAP_MDS_H
#define _SNAP_MDS_H
#include <string>
#include <mutex>
#include <grpc++/grpc++.h>
#include "../rpc/snapshot.pb.h"
#include "../rpc/snapshot.grpc.pb.h"
#include "../log/log.h"

#include "snapshot_type.h"
#include "block_store.h"
#include "index_store.h"

using huawei::proto::GetUidReq;
using huawei::proto::GetUidAck;
using huawei::proto::CreateReq;
using huawei::proto::CreateAck;
using huawei::proto::ListReq;
using huawei::proto::ListAck;
using huawei::proto::DeleteReq;
using huawei::proto::DeleteAck;
using huawei::proto::RollbackReq;
using huawei::proto::RollbackAck;
using huawei::proto::SetStatusReq;
using huawei::proto::SetStatusAck;
using huawei::proto::GetStatusReq;
using huawei::proto::GetStatusAck;
using huawei::proto::CowReq;
using huawei::proto::CowAck;
using huawei::proto::CowUpdateReq;
using huawei::proto::CowUpdateAck;
using huawei::proto::DiffReq;
using huawei::proto::DiffAck;
using huawei::proto::ReadReq;
using huawei::proto::ReadAck;
using huawei::proto::GetSnapshotNameReq;
using huawei::proto::GetSnapshotNameAck;
using huawei::proto::GetSnapshotIdReq;
using huawei::proto::GetSnapshotIdAck;

/*work on storage gateway server, hold each volume snapshot meta data*/
class SnapshotMds
{
public:
    SnapshotMds(string vol_name);
    ~SnapshotMds();   

    /*spawn snapshot unique id*/ 
    int get_uid(const GetUidReq* req, GetUidAck* ack);

    int get_snapshot_name(const GetSnapshotNameReq* req, GetSnapshotNameAck* ack);
    int get_snapshot_id(const GetSnapshotIdReq* req, GetSnapshotIdAck* ack);

    /*snapshot common operation*/
    int create_snapshot(const CreateReq* req, CreateAck* ack);
    int list_snapshot(const ListReq* req, ListAck* ack);
    int delete_snapshot(const DeleteReq* req, DeleteAck* ack);
    int rollback_snapshot(const RollbackReq* req, RollbackAck* ack);
    int diff_snapshot(const DiffReq* req, DiffAck* ack);    
    int read_snapshot(const ReadReq* req, ReadAck* ack);
    
    /*snapshot status*/
    int set_status(const SetStatusReq* req, SetStatusAck* ack);
    int get_status(const GetStatusReq* req, GetStatusAck* ack);
    
    /*cow*/
    int cow_op(const CowReq* req, CowAck* ack);
    int cow_update(const CowUpdateReq* req, CowUpdateAck* ack);
    
    /*crash recover*/
    int recover();

private:
    /*create snapshot update status*/
    int create_update_status(snapid_t snap_id, snapshot_status_t snap_status);
    /*delete snapshot update status*/
    int delete_update_status(snapid_t snap_id, snapshot_status_t snap_status);
    /*get snapshot name by snapshot id*/
    string get_snapshot_name(snapid_t snap_id);

    /*helper to generate cow object name*/
    string spawn_cow_object_name(const snapid_t snap_id, const block_t blk_id);
    void   split_cow_object_name(const string& raw, 
                                 string&   vol_name,
                                 snapid_t& snap_id, 
                                 block_t&  blk_id);

    /*helper to persist maps to indexstore*/
    string spawn_key(const string& prefix, const string& value);
    void   split_key(const string& raw, string& prefix, string& key); 

    string spawn_snapshot_map_key(const string& snap_name);
    void   split_snapshot_map_key(const string& raw_key, string& snap_name);
    
    string spawn_status_map_key(const snapid_t& snap_id);
    void   split_status_map_key(const string& raw_key, snapid_t& snap_id);

    string spawn_cow_block_map_key(const snapid_t& snap_id,
                                   const block_t& block_id);
    void   split_cow_block_map_key(const string& raw_key, 
                                   snapid_t& snap_id, 
                                   block_t& block_id);

    string spawn_cow_object_map_key(const string& obj_name);
    void   split_cow_object_map_key(const string& raw_key, string obj_name);
    
    string spawn_cow_object_map_val(const cow_object_ref_t& obj_ref);
    void   split_cow_object_map_val(const string& raw_val, 
                                    cow_object_ref_t& obj_ref);
    /*debug*/
    void trace();

private:
    /*volume name*/
    string m_volume_name;
    /*lock*/
    mutex m_mutex;
    /*the latest snapshot id*/
    snapid_t m_latest_snapid;
    /*snapshot name and snapshot id map*/
    map<string, snapid_t> m_snapshots;
    /*snapshot and status map*/
    map<snapid_t, snapshot_status_t> m_snapshots_status;
    /*snapshot id and snapshot cow block map*/
    map<snapid_t, map<block_t, cow_object_t>> m_cow_block_map;
    /*cow object and snapshot referece map*/
    map<cow_object_t, cow_object_ref_t> m_cow_object_map;

    /*index store for snapshot meta persist*/
    IndexStore* m_index_store;
    /*block store for cow object*/
    BlockStore* m_block_store;
};

#endif
