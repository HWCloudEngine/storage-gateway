#ifndef _SNAP_MDS_H
#define _SNAP_MDS_H
#include <string>
#include <mutex>
#include <grpc++/grpc++.h>
#include "../rpc/common.pb.h"
#include "../rpc/snapshot.pb.h"
#include "../rpc/snapshot_inner_control.pb.h"
#include "../rpc/snapshot_inner_control.grpc.pb.h"
#include "../log/log.h"
#include "snapshot_type.h"
#include "block_store.h"
#include "index_store.h"
#include "abstract_mds.h"

using huawei::proto::SnapStatus;
using huawei::proto::SnapReqHead;

class SnapshotMds : public AbstractMds
{
public:
    SnapshotMds(string vol_name);
    virtual ~SnapshotMds();   

    /*storage client sync snapshot status*/ 
    StatusCode sync(const SyncReq* req, SyncAck* ack);

    /*snapshot common operation*/
    virtual StatusCode create_snapshot(const CreateReq* req, CreateAck* ack);
    virtual StatusCode delete_snapshot(const DeleteReq* req, DeleteAck* ack);
    virtual StatusCode rollback_snapshot(const RollbackReq* req, RollbackAck* ack);
    virtual StatusCode list_snapshot(const ListReq* req, ListAck* ack);
    virtual StatusCode query_snapshot(const QueryReq* req, QueryAck* ack);
    virtual StatusCode diff_snapshot(const DiffReq* req, DiffAck* ack);    
    virtual StatusCode read_snapshot(const ReadReq* req, ReadAck* ack);
    
    /*snapshot status*/
    virtual StatusCode update(const UpdateReq* req, UpdateAck* ack);
    
    /*cow*/
    virtual StatusCode cow_op(const CowReq* req, CowAck* ack);
    virtual StatusCode cow_update(const CowUpdateReq* req, CowUpdateAck* ack);
    
    /*crash recover*/
    int recover();

private:
    /*common helper*/
    snapid_t spawn_snapshot_id();
    snapid_t get_snapshot_id(string snap_name);
    string   get_snapshot_name(snapid_t snap_id);
    
    /*accord local and remote to mapping snapshot pair*/
    string mapping_snap_name(const SnapReqHead& shead, const string& sname);
   
    /*maintain snapshot status*/
    StatusCode created_update_status(string snap_name);
    StatusCode deleted_update_status(string snap_name);

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

    /*snapshot and attr map*/
    map<string, snap_attr_t> m_snapshots;
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
