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
#include "../common/blocking_queue.h"
#include "../log/log.h"
#include "../sg_client/journal_entry.h"
#include "../rpc/common.pb.h"
#include "../rpc/snapshot_control.pb.h"
#include "../rpc/snapshot_control.grpc.pb.h"
#include "../rpc/snapshot_inner_control.pb.h"
#include "../rpc/snapshot_inner_control.grpc.pb.h"
#include "snapshot_type.h"
#include "block_store.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using huawei::proto::StatusCode;
using huawei::proto::SnapStatus;

using huawei::proto::control::CreateSnapshotReq;
using huawei::proto::control::CreateSnapshotAck;
using huawei::proto::control::ListSnapshotReq;
using huawei::proto::control::ListSnapshotAck;
using huawei::proto::control::QuerySnapshotReq;
using huawei::proto::control::QuerySnapshotAck;
using huawei::proto::control::RollbackSnapshotReq;
using huawei::proto::control::RollbackSnapshotAck;
using huawei::proto::control::DeleteSnapshotReq;
using huawei::proto::control::DeleteSnapshotAck;
using huawei::proto::control::DiffSnapshotReq;
using huawei::proto::control::DiffSnapshotAck;
using huawei::proto::control::ReadSnapshotReq;
using huawei::proto::control::ReadSnapshotAck;
using huawei::proto::control::ExtDiffBlock;
using huawei::proto::control::ExtDiffBlocks; 

using huawei::proto::inner::SnapshotInnerControl;

using namespace std;

/*work on storage gateway client, each volume own a SnapshotProxy*/
class SnapshotProxy{
public:
    SnapshotProxy(string volume_id,
                  string block_device,
                  BlockingQueue<shared_ptr<JournalEntry>>& entry_queue)
        :m_volume_id(volume_id), 
        m_block_device(block_device),
        m_entry_queue(entry_queue){
        LOG_INFO << "SnapshotProxy create vid:" << volume_id  \
                 << " blk:" << block_device;
        init();
    }

    ~SnapshotProxy(){
        LOG_INFO << "SnapshotProxy destroy vid:" << m_volume_id \
                 << " blk:" << m_block_device;
        fini();
    }

    bool init();
    bool fini();
    
    /*crash recover, synchronize snapshot status with dr_server*/
    StatusCode sync_state();

    /*called by control layer*/
    StatusCode create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack);
    StatusCode delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack);
    StatusCode rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack);
    StatusCode list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack);
    StatusCode query_snapshot(const QuerySnapshotReq* req, QuerySnapshotAck* ack);
    StatusCode diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack);
    StatusCode read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack);

    /*rpc with dr server*/
    StatusCode do_create(string snap_name);
    StatusCode do_delete(string snap_name);  
    StatusCode do_cow(const off_t& off, const size_t& size, char* buf, bool rollback);
    StatusCode do_rollback(string snap_name);
    StatusCode do_update(const string& snap_name);

    /*make sure journal writer persist ok then ack to client*/
    int  cmd_persist_wait();
    void cmd_persist_notify();
    
    /*call by journal replayer*/
    StatusCode create_transaction(string snap_name);
    StatusCode delete_transaction(string snap_name);
    StatusCode rollback_transaction(string snap_name);
    bool check_exist_snapshot()const;

private:
    /*split io into fixed size block*/
    void split_cow_block(const off_t& off, const size_t& size,
                         vector<cow_block_t>& cow_blocks);
    /*block device read and write*/
    size_t raw_device_write(char* buf, size_t len, off_t off);
    size_t raw_device_read(char* buf, size_t len, off_t off);
    
    /*accord message type to spawn journal entry*/
    shared_ptr<JournalEntry> spawn_journal_entry(string snap_name,
                                                 journal_event_type_t type);
    /*common transaction mechanism*/
    StatusCode transaction(string snap_name) ; 

private:
    /*volume name*/
    string m_volume_id;
    /*block device name*/
    string m_block_device;
    /*block device read write fd*/
    int    m_block_fd;

    /*entry queue to journal preprocessor*/
    BlockingQueue<shared_ptr<JournalEntry>>& m_entry_queue;
    
    /*sync between writer and proxy*/
    mutex  m_cmd_persit_lock;
    condition_variable m_cmd_persit_cond;

    /*local store snapshot status*/
    map<string, SnapStatus> m_snapshots;
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
