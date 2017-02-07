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
#include "../rpc/snapshot.pb.h"
#include "../rpc/journal.pb.h"
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
using huawei::proto::SnapReqHead;
using huawei::proto::JournalMarker;
using huawei::proto::DiffBlocks; 

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

    /*call by journal replayer*/
    StatusCode create_transaction(const SnapReqHead& shead, const string& snap_name);
    StatusCode delete_transaction(const SnapReqHead& shead, const string& snap_name);
    StatusCode rollback_transaction(const SnapReqHead& shead, const string& snap_name);
    bool check_exist_snapshot()const;

    /*rpc with dr server*/
    StatusCode do_create(const SnapReqHead& shead, const string& sname);
    StatusCode do_delete(const SnapReqHead& shead, const string& sname);
    StatusCode do_cow(const off_t& off, const size_t& size, char* buf, bool rollback);
    StatusCode do_update(const SnapReqHead& shead, const string& sname);
    StatusCode do_rollback(const SnapReqHead& shead, const string& sname);

    /*make sure journal writer persist ok then ack to client*/
    void cmd_persist_wait();
    void cmd_persist_notify(const JournalMarker& mark);
    
private:
    /*split io into fixed size block*/
    void split_cow_block(const off_t& off, const size_t& size,
                         vector<cow_block_t>& cow_blocks);
    /*block device read and write*/
    size_t raw_device_write(char* buf, size_t len, off_t off);
    size_t raw_device_read(char* buf, size_t len, off_t off);
    
    /*accord message type to spawn journal entry*/
    shared_ptr<JournalEntry> spawn_journal_entry(const SnapReqHead& shead,
                                                 const string& sname,
                                                 const journal_event_type_t& entry_type);

    /*common transaction mechanism*/
    StatusCode transaction(const SnapReqHead& shead, const string& snap_name); 
   
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
    mutex              m_cmd_persist_lock;
    condition_variable m_cmd_persist_cond;
    JournalMarker      m_cmd_persist_mark;

    /*local store snapshot attribute*/
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
