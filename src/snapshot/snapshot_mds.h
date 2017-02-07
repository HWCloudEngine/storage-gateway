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
#include "../common/block_store.h"
#include "../common/index_store.h"
#include "snapshot_type.h"

using huawei::proto::StatusCode;
using huawei::proto::SnapStatus;
using huawei::proto::SnapReqHead;

using huawei::proto::inner::CreateReq;
using huawei::proto::inner::CreateAck;
using huawei::proto::inner::ListReq;
using huawei::proto::inner::ListAck;
using huawei::proto::inner::QueryReq;
using huawei::proto::inner::QueryAck;
using huawei::proto::inner::DeleteReq;
using huawei::proto::inner::DeleteAck;
using huawei::proto::inner::RollbackReq;
using huawei::proto::inner::RollbackAck;
using huawei::proto::inner::UpdateReq;
using huawei::proto::inner::UpdateAck;
using huawei::proto::inner::CowReq;
using huawei::proto::inner::CowAck;
using huawei::proto::inner::CowUpdateReq;
using huawei::proto::inner::CowUpdateAck;
using huawei::proto::inner::DiffReq;
using huawei::proto::inner::DiffAck;
using huawei::proto::inner::ReadReq;
using huawei::proto::inner::ReadAck;
using huawei::proto::inner::SyncReq;
using huawei::proto::inner::SyncAck;


class SnapshotMds
{
public:
    SnapshotMds(const string& vol_name, const size_t& vol_size);
    virtual ~SnapshotMds();   

    /*storage client sync snapshot status*/ 
    StatusCode sync(const SyncReq* req, SyncAck* ack);

    /*snapshot common operation*/
    StatusCode create_snapshot(const CreateReq* req, CreateAck* ack);
    StatusCode delete_snapshot(const DeleteReq* req, DeleteAck* ack);
    StatusCode rollback_snapshot(const RollbackReq* req, RollbackAck* ack);
    StatusCode list_snapshot(const ListReq* req, ListAck* ack);
    StatusCode query_snapshot(const QueryReq* req, QueryAck* ack);
    StatusCode diff_snapshot(const DiffReq* req, DiffAck* ack);    
    StatusCode read_snapshot(const ReadReq* req, ReadAck* ack);
    
    /*snapshot status*/
    StatusCode update(const UpdateReq* req, UpdateAck* ack);
    
    /*cow*/
    StatusCode cow_op(const CowReq* req, CowAck* ack);
    StatusCode cow_update(const CowUpdateReq* req, CowUpdateAck* ack);
    
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
    size_t m_volume_size;
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
