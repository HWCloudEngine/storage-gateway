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

using huawei::proto::CreateReq;
using huawei::proto::CreateAck;
using huawei::proto::ListReq;
using huawei::proto::ListAck;
using huawei::proto::DeleteReq;
using huawei::proto::DeleteAck;
using huawei::proto::RollbackReq;
using huawei::proto::RollbackAck;
using huawei::proto::UpdateReq;
using huawei::proto::UpdateAck;
using huawei::proto::CowReq;
using huawei::proto::CowAck;
using huawei::proto::CowUpdateReq;
using huawei::proto::CowUpdateAck;
using huawei::proto::DiffReq;
using huawei::proto::DiffAck;
using huawei::proto::ReadReq;
using huawei::proto::ReadAck;
using huawei::proto::SyncReq;
using huawei::proto::SyncAck;

/*helper function to handle db persist key*/
class DbUtil
{
public:
    static string spawn_key(const string& prefix, const string& value);
    static void   split_key(const string& raw, string& prefix, string& key); 
    static string spawn_latest_id_key();
    static string spawn_latest_name_key();
    static string spawn_ids_map_key(const string& snap_name);
    static void   split_ids_map_key(const string& raw_key, string& snap_name);
    static string spawn_status_map_key(const string& snap_name);
    static void   split_status_map_key(const string& raw_key, string& snap_name);
    static string spawn_cow_block_map_key(const snapid_t& snap_id,
                                          const block_t& block_id);
    static void   split_cow_block_map_key(const string& raw_key, 
                                          snapid_t& snap_id, 
                                          block_t& block_id);
    static string spawn_cow_object_map_key(const string& obj_name);
    static void   split_cow_object_map_key(const string& raw_key, string obj_name);
    static string spawn_cow_object_map_val(const cow_object_ref_t& obj_ref);
    static void   split_cow_object_map_val(const string& raw_val, 
                                           cow_object_ref_t& obj_ref);
};

/*work on storage gateway server, hold each volume snapshot meta data*/
class SnapshotMds
{
public:
    SnapshotMds(string vol_name);
    ~SnapshotMds();   

    /*storage client sync snapshot status*/ 
    int sync(const SyncReq* req, SyncAck* ack);

    /*snapshot common operation*/
    int create_snapshot(const CreateReq* req, CreateAck* ack);
    int delete_snapshot(const DeleteReq* req, DeleteAck* ack);
    int rollback_snapshot(const RollbackReq* req, RollbackAck* ack);
    int list_snapshot(const ListReq* req, ListAck* ack);
    int diff_snapshot(const DiffReq* req, DiffAck* ack);    
    int read_snapshot(const ReadReq* req, ReadAck* ack);
    
    /*snapshot status*/
    int update(const UpdateReq* req, UpdateAck* ack);
    
    /*cow*/
    int cow_op(const CowReq* req, CowAck* ack);
    int cow_update(const CowUpdateReq* req, CowUpdateAck* ack);
    
    /*crash recover*/
    int recover();

private:
    /*common helper*/
    snapid_t spawn_snapshot_id();
    snapid_t get_snapshot_id(string snap_name);
    string   get_snapshot_name(snapid_t snap_id);
    
    /*maintain snapshot status*/
    int created_update_status(string snap_name);
    int deleted_update_status(string snap_name);

    /*helper to generate cow object name*/
    string spawn_cow_object_name(const snapid_t snap_id, const block_t blk_id);
    void   split_cow_object_name(const string& raw, string& vol_name,
                                 snapid_t& snap_id, block_t&  blk_id);
    /*debug*/
    void trace();

private:
    /*volume name*/
    string m_volume_name;
    /*lock*/
    mutex m_mutex;
    /*the latest snapshot id*/
    snapid_t m_latest_snapid;
    string   m_latest_snapname;
    /*snapshot and status map*/
    map<string, snapshot_status_t> m_snapshots_status;
    /*snapshot name and snapshot id map*/
    map<string, snapid_t> m_snapshots_ids;
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
