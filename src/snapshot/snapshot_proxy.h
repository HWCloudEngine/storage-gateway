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
#include "../common/prqueue.h"
#include "../log/log.h"
#include "../journal/journal_entry.hpp"
#include "../rpc/control.pb.h"
#include "../rpc/control.grpc.pb.h"
#include "../rpc/snapshot.pb.h"
#include "../rpc/snapshot.grpc.pb.h"
#include "snapshot_type.h"
#include "block_store.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using huawei::proto::SnapshotRpcSvc;
using huawei::proto::CreateSnapshotReq;
using huawei::proto::CreateSnapshotAck;
using huawei::proto::ListSnapshotReq;
using huawei::proto::ListSnapshotAck;
using huawei::proto::RollbackSnapshotReq;
using huawei::proto::RollbackSnapshotAck;
using huawei::proto::DeleteSnapshotReq;
using huawei::proto::DeleteSnapshotAck;
using huawei::proto::DiffSnapshotReq;
using huawei::proto::DiffSnapshotAck;
using huawei::proto::ReadSnapshotReq;
using huawei::proto::ReadSnapshotAck;
using huawei::proto::ExtDiffBlock;
using huawei::proto::ExtDiffBlocks; 

using namespace std;

/*work on storage gateway client, each volume own a SnapshotProxy*/
class SnapshotProxy{
public:
    SnapshotProxy(string volume_id,
                  string block_device,
                  PRQueue<shared_ptr<JournalEntry>>& entry_queue)
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
    int sync_state();

    /*called by control layer*/
    int create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack);
    int delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack);
    int rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack);
    int list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack);
    int diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack);
    int read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack);

    /*rpc with dr server*/
    int do_create(string snap_name);
    int do_delete(string snap_name);  
    int do_cow(const off_t& off, const size_t& size, char* buf, bool rollback);
    int do_rollback(string snap_name);
    int do_update(const string& snap_name);

    /*make sure journal writer persist ok then ack to client*/
    int  cmd_persist_wait();
    void cmd_persist_notify();
    
    /*call by journal replayer*/
    int create_transaction(string snap_name);
    int delete_transaction(string snap_name);
    int rollback_transaction(string snap_name);
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
    int transaction(string snap_name) ; 

private:
    /*volume name*/
    string m_volume_id;
    /*block device name*/
    string m_block_device;
    /*block device read write fd*/
    int    m_block_fd;

    /*entry queue to journal preprocessor*/
    PRQueue<shared_ptr<JournalEntry>>& m_entry_queue;
    
    /*sync between writer and proxy*/
    mutex  m_cmd_persit_lock;
    condition_variable m_cmd_persit_cond;

    /*local store snapshot status*/
    map<string, snapshot_status_t> m_snapshots;
    /*current active snapshot*/
    string  m_active_snapshot;
    /*check now exist snapshot or not*/
    atomic_bool m_exist_snapshot;

    /*snapshot block store*/
    BlockStore* m_block_store;

    /*rpc interact with dr server, snapshot meta data access*/
    unique_ptr<SnapshotRpcSvc::Stub> m_rpc_stub;
};

#endif
