#ifndef _SNAPSHOT_PROXY_H
#define _SNAPSHOT_PROXY_H
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <set>
#include <memory>
#include <string>
#include <condition_variable>
#include <atomic>
#include <grpc++/grpc++.h>
#include "log/log.h"
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/journal.pb.h"
#include "rpc/snapshot_control.pb.h"
#include "rpc/snapshot_control.grpc.pb.h"
#include "rpc/snapshot_inner_control.pb.h"
#include "rpc/snapshot_inner_control.grpc.pb.h"
#include "rpc/clients/backup_inner_ctrl_client.h"
#include "common/define.h"
#include "common/blocking_queue.h"
#include "common/block_store.h"
#include "common/volume_attr.h"
#include "common/define.h"
#include "snapshot.h"
#include "syncbarrier.h"
#include "transaction.h"
#include "sg_client/journal_entry.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::VolumeInfo;
using huawei::proto::StatusCode;
using huawei::proto::SnapType;
using huawei::proto::SnapStatus;
using huawei::proto::SnapReqHead;
using huawei::proto::JournalMarker;
using huawei::proto::DiffBlocks; 
using huawei::proto::inner::SnapshotInnerControl;
using huawei::proto::inner::UpdateEvent;

using namespace std;


/*work on storage gateway client, each volume own a SnapshotProxy*/
class SnapshotProxy : public ISnapshot, public ITransaction, public ISyncBarrier
{
public:
    SnapshotProxy(VolumeAttr& vol_attr, 
                  BlockingQueue<shared_ptr<JournalEntry>>& entry_queue)
        :m_vol_attr(vol_attr), 
        m_entry_queue(entry_queue){
        LOG_INFO << "create vname:" << m_vol_attr.vol_name() << " blk:" << m_vol_attr.blk_device();
        init();
    }

    ~SnapshotProxy(){
        LOG_INFO << "delete vname:" << m_vol_attr.vol_name() << " blk:" << m_vol_attr.blk_device();
        fini();
    }

    bool init();
    bool fini();
    
    /*crash recover, synchronize snapshot status with dr_server*/
    StatusCode sync_state();

    /*called by control layer*/
    StatusCode create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack) override;
    StatusCode delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack) override;
    StatusCode rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack) override;
    StatusCode list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack)override;
    StatusCode query_snapshot(const QuerySnapshotReq* req, QuerySnapshotAck* ack) override;
    StatusCode diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack) override;
    StatusCode read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack) override;

    /*call by journal replayer*/
    StatusCode create_transaction(const SnapReqHead& shead, const string& snap_name) override;
    StatusCode delete_transaction(const SnapReqHead& shead, const string& snap_name) override;
    StatusCode rollback_transaction(const SnapReqHead& shead, const string& snap_name) override;

    bool check_exist_snapshot()const;

    /*rpc with dr server*/
    StatusCode do_create(const SnapReqHead& shead, const string& sname);
    StatusCode do_delete(const SnapReqHead& shead, const string& sname);
    StatusCode do_cow(const off_t& off, const size_t& size, char* buf, bool rollback);
    StatusCode do_update(const SnapReqHead& shead, const string& sname, const UpdateEvent& sevent);
    StatusCode do_rollback(const SnapReqHead& shead, const string& sname);

    /*make sure journal writer persist ok then ack to client*/
    void cmd_persist_wait();
    void cmd_persist_notify(const JournalMarker& mark);
    
    void add_sync(const string& actor, const string& action) override;
    void del_sync(const string& actor) override;
    bool check_sync_on(const string& actor) override;
    
private:
    /*split io into fixed size block*/
    struct cow_block 
    {
        off_t   off;
        size_t  len;
        block_t blk_no;
    };
    typedef struct cow_block cow_block_t;
    void split_cow_block(const off_t& off, const size_t& size, vector<cow_block_t>& cow_blocks);
    /*block device read and write*/
    size_t raw_device_write(char* buf, size_t len, off_t off);
    size_t raw_device_read(char* buf, size_t len, off_t off);
    
    /*accord message type to spawn journal entry*/
    shared_ptr<JournalEntry> spawn_journal_entry(const SnapReqHead& shead,
                                                 const string& sname,
                                                 const journal_event_type_t& entry_type);

    /*common transaction mechanism*/
    StatusCode transaction(const SnapReqHead& shead, const string& sname, const UpdateEvent& sevent); 
   
private:
    /*volume attr*/
    VolumeAttr& m_vol_attr;

    /*block device read write fd*/
    int m_block_fd;

    /*entry queue to journal preprocessor*/
    BlockingQueue<shared_ptr<JournalEntry>>& m_entry_queue;
    
    /*sync between writer and proxy*/
    mutex              m_cmd_persist_lock;
    condition_variable m_cmd_persist_cond;
    JournalMarker      m_cmd_persist_mark;
    
    /*backup inner rpc client*/
    BackupInnerCtrlClient* m_backup_inner_rpc_client;
    
    /*sync table*/
    map<string, string> m_sync_table;

    /*current active snapshot*/
    string  m_active_snapshot;

    /*check now exist snapshot or not*/
    atomic_bool m_exist_snapshot;

    /*snapshot block store*/
    BlockStore* m_block_store;

    /*rpc interact with dr server, snapshot meta data access*/
    unique_ptr<SnapshotInnerControl::Stub> m_rpc_stub;
};

#endif
