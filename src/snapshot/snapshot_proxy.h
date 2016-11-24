#ifndef _SNAPSHOT_PROXY_H
#define _SNAPSHOT_PROXY_H
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <set>
#include <memory>
#include <string>
#include <condition_variable>
#include <grpc++/grpc++.h>
#include "../journal/replay_entry.hpp"
#include "../log/log.h"
#include "../rpc/control.pb.h"
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
using huawei::proto::GetSnapshotNameReq;
using huawei::proto::GetSnapshotNameAck;
using huawei::proto::GetSnapshotIdReq;
using huawei::proto::GetSnapshotIdAck;

using namespace std;
using namespace Journal;

/*work on storage gateway client, each volume own a SnapshotProxy*/
class SnapshotProxy{
public:
    SnapshotProxy(string volume_id,
                  string block_device,
                  nedalloc::nedpool*  buffer_pool,
                  entry_queue&        entry_que, 
                  condition_variable& entry_cv)
        :m_volume_id(volume_id), 
         m_block_device(block_device),
         m_buffer_pool(buffer_pool),
         m_entry_queue(entry_que), 
         m_entry_cv(entry_cv){
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

    /*called by control layer*/
    int create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack);
    int list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack);
    int delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack);
    int rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack);
    int diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack);
    int read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack);

    /*snapshot internal use*/
    string   remote_get_snapshot_name(snapid_t snap_id);
    snapid_t remote_get_snapshot_id(string snap_name);
    /*todo: consistence problem*/
    string   local_get_snapshot_name(snapid_t snap_id);
    snapid_t local_get_snapshot_id(string snap_name);

    /*snapshot status*/
    int set_status(const snapid_t& snap_id, 
                   const enum huawei::proto::SnapshotStatus& snap_status);
    int get_status(const snapid_t& snap_id, 
                   enum huawei::proto::SnapshotStatus& snap_status);
    
    /*called by journal replayer only*/
    int do_create(string snap_name, snapid_t snap_id);
    int do_delete(snapid_t snap_id);  
    int do_cow(const off_t& off, const size_t& size, char* buf, bool rollback);
    int do_rollback(snapid_t snap_id);

private:
    /*split io into fixed size block*/
    void split_cow_block(const off_t& off, const size_t& size,
                         vector<cow_block_t>& cow_blocks);
    /*block device read and write*/
    size_t raw_device_write(char* buf, size_t len, off_t off);
    size_t raw_device_read(char* buf, size_t len, off_t off);

private:
    /*volume name*/
    string m_volume_id;
    /*block device name*/
    string m_block_device;
    /*block device read write fd*/
    int    m_block_fd;

    /*entry queue and buffer pool shared with connection*/
    nedalloc::nedpool*  m_buffer_pool;
    entry_queue&        m_entry_queue;
    condition_variable& m_entry_cv;

    /*todo: snap name map to snapid(necessary ?)*/
    map<string, snapid_t> m_snapshots;
    map<snapid_t, string> m_snapshots_ids;

    /*todo: current active snapshot(necessary ?)*/
    snapid_t            m_active_snapid;
    
    /*snapshot block store*/
    BlockStore*        m_block_store;

    /*rpc interact with dr server, snapshot meta data access*/
    unique_ptr<SnapshotRpcSvc::Stub> m_rpc_stub;
};

#endif
