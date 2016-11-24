#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <set>
#include "interval_set.h"
#include "snapshot_proxy.h"

using huawei::proto::GetUidReq;
using huawei::proto::GetUidAck;
using huawei::proto::CreateReq;
using huawei::proto::CreateAck;
using huawei::proto::ListReq;
using huawei::proto::ListAck;
using huawei::proto::DeleteReq;
using huawei::proto::DeleteAck;
using huawei::proto::SetStatusReq;
using huawei::proto::SetStatusAck;
using huawei::proto::GetStatusReq;
using huawei::proto::GetStatusAck;
using huawei::proto::CowReq;
using huawei::proto::CowAck;
using huawei::proto::CowUpdateReq;
using huawei::proto::CowUpdateAck;
using huawei::proto::RollbackReq;
using huawei::proto::RollbackAck;
using huawei::proto::DiffReq;
using huawei::proto::DiffAck;
using huawei::proto::ReadReq;
using huawei::proto::ReadAck;

bool SnapshotProxy::init()
{        
    /*rpc connect*/
    m_rpc_stub = SnapshotRpcSvc::NewStub(grpc::CreateChannel(
                "127.0.0.1:50051", 
                grpc::InsecureChannelCredentials()));
    
    /*open block device*/
    m_block_fd = open(m_block_device.c_str(), O_RDWR | O_DIRECT | O_SYNC);
    if(m_block_fd == -1){
        LOG_ERROR << "open block device:" << m_block_device.c_str() 
                  << " errno:" << errno; 
        return false;
    }
    
    LOG_INFO << "open block device:" << m_block_device.c_str()
             << " m_block_fd:" << m_block_fd;

    /*snapshot block store*/
    m_block_store = new CephBlockStore();
    return true;
}

bool SnapshotProxy::fini()
{
    if(m_block_store){
        delete m_block_store;
    }

    if(m_block_fd != -1){
        close(m_block_fd); 
    }
    return true;
}

int SnapshotProxy::create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack)
{
    /*get from exterior rpc*/
    string vname = req->vol_name();
    string sname = req->snap_name();

    LOG_INFO << "SnapshotProxy create_snapshot"
             << " vname:" << vname 
             << " sname:" << sname;
    
    /*todo: here may exist atomic or consistence problem*/

    /*only get available snapshot uid*/
    ClientContext context;
    GetUidReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    GetUidAck iack;
    Status st = m_rpc_stub->GetUid(&context, ireq, &iack);
    LOG_INFO << "SnapshotProxy GetUid"
             << " ret:" << iack.ret()
             << " snap_id:" << iack.snap_id();

    /*todo: temporary save for latter use*/
    m_snapshots.insert(pair<string, snapid_t>(sname, iack.snap_id())) ;
    m_snapshots_ids.insert(pair<snapid_t, string>(iack.snap_id(), sname));

    /*append to journal*/
    log_header_t* snap_entry = (log_header_t*)nedalloc::nedpmalloc(m_buffer_pool, 
                                              sizeof(log_header_t));
    snap_entry->type  = SNAPSHOT_CREATE;
    snap_entry->count = iack.snap_id();
    entry_ptr entry= new ReplayEntry((char*)snap_entry, sizeof(log_header_t), 
                                     0, m_buffer_pool);
    assert(entry != nullptr);
    m_entry_queue.push(entry);
    m_entry_cv.notify_all();

    LOG_INFO << "SnapshotProxy create_snapshot"
             << " vname:" << vname 
             << " sname:" << sname
             << " ok";
    return 0;
}

int SnapshotProxy::list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack)
{
    string vname = req->vol_name();
    LOG_INFO << "SnapshotProxy list_snapshot"
             << " vname:" << vname;

    ClientContext context;
    ListReq ireq;
    ireq.set_vol_name(vname);
    ListAck iack;
    Status st = m_rpc_stub->List(&context, ireq, &iack);
    int snap_num = iack.snap_name_size();
    for(int i = 0; i < snap_num; i++){
       ack->add_snap_name(iack.snap_name(i)); 
    }
    LOG_INFO << "SnapshotProxy list_snapshot"
             << " vname:" << vname
             << " snap_size:" << iack.snap_name_size() 
             << " ok";
    return 0; 
}

int SnapshotProxy::delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "SnapshotProxy delete_snapshot"
             << " vname:" << vname
             << " sname:" << sname;
    
    /*append to journal entry*/
    log_header_t* snap_entry = (log_header_t*)nedalloc::nedpmalloc(m_buffer_pool,
                                                    sizeof(log_header_t));
    snap_entry->type  = SNAPSHOT_DELETE;
    /*todo: need get from dr server*/
    snap_entry->count = m_snapshots[sname];
    entry_ptr entry= new ReplayEntry((char*)snap_entry, sizeof(log_header_t), 
                                     0, m_buffer_pool);
    assert(entry != nullptr);
    m_entry_queue.push(entry);
    m_entry_cv.notify_all();
    
    /*only tell dr server update snapshot status*/
    ClientContext context;
    SetStatusReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_snap_id(m_snapshots[sname]);
    ireq.set_snap_status(::huawei::proto::SNAPSHOT_DELETING);
    SetStatusAck iack;
    Status st = m_rpc_stub->SetStatus(&context, ireq, &iack);

    LOG_INFO << "SnapshotProxy delete_snapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " ret:"   << iack.ret();
    return 0; 
}


int SnapshotProxy::do_create(string snap_name, snapid_t snap_id)
{
    LOG_INFO << "SnapshotProxy do_create"
             << " snap_name:" << snap_name
             << " snap_id:"   << snap_id;

    /*dr server really create snapshot*/
    ClientContext context;
    CreateReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_name(snap_name);
    ireq.set_snap_id(snap_id);
    CreateAck iack;
    Status st = m_rpc_stub->Create(&context, ireq, &iack);
        
    /*update latest snapshot id*/
    m_active_snapid = snap_id;

    LOG_INFO << "SnapshotProxy do_create"
             << " snap_name:" << snap_name
             << " snap_id:" << snap_id
             << " ret:"   << iack.ret();
    return 0;
}

int SnapshotProxy::do_delete(snapid_t snap_id)
{
    LOG_INFO << "SnapshotProxy do_delete"
             << " snap_id:" << snap_id;
    
    /*really tell dr server delete snapshot*/
    ClientContext context;
    DeleteReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_id(snap_id);
    DeleteAck iack;
    Status st = m_rpc_stub->Delete(&context, ireq, &iack);

    LOG_INFO << "SnapshotProxy do_delete"
             << " snap_id:" << snap_id
             << " ret:"   << iack.ret();
    return 0;
}

int SnapshotProxy::rollback_snapshot(const RollbackSnapshotReq* req, 
                                     RollbackSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();

    LOG_INFO << "SnapshotProxy rollback_snapshot"
             << " vname:" << vname
             << " sname:" << sname;
    
    /*append to journal entry*/
    log_header_t* snap_entry = (log_header_t*)nedalloc::nedpmalloc(m_buffer_pool,
                                                 sizeof(log_header_t));
    snap_entry->type  = SNAPSHOT_ROLLBACK;
    snap_entry->count = m_snapshots[sname];
    entry_ptr entry= new ReplayEntry((char*)snap_entry, sizeof(log_header_t), 
                                      0, m_buffer_pool);
    assert(entry != nullptr);
    m_entry_queue.push(entry);
    m_entry_cv.notify_all();
    
    /*only tell dr server update snapshot status*/
    ClientContext context;
    SetStatusReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_snap_id(m_snapshots[sname]);
    ireq.set_snap_status(::huawei::proto::SNAPSHOT_ROLLBACKING);
    SetStatusAck iack;
    Status st = m_rpc_stub->SetStatus(&context, ireq, &iack);

    LOG_INFO << "SnapshotProxy rollback_snapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " ret:"   << iack.ret();
    return 0;
}

int SnapshotProxy::do_rollback(snapid_t snap_id)
{
    LOG_INFO << "SnapshotProxy do rollback" 
             << " snap_id: " << snap_id;
    ClientContext context;
    RollbackReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_id(snap_id);
    RollbackAck iack;
    Status st = m_rpc_stub->Rollback(&context, ireq, &iack);
    
    int roll_blk_num = iack.roll_blocks_size();
    for(int i = 0; i < roll_blk_num; i++){
        huawei::proto::RollBlock roll_block = iack.roll_blocks(i);
        LOG_INFO << "SnapshotProxy do rollback" 
                 << " blk_no:" << roll_block.blk_no() 
                 << " blk_object:" << roll_block.blk_object(); 

        /*read latest data from block device*/
        void* block_buf = nullptr;
        int ret = posix_memalign((void**)&block_buf, 512, COW_BLOCK_SIZE);
        assert(ret == 0 && block_buf != nullptr);
        off_t block_off  = roll_block.blk_no() * COW_BLOCK_SIZE;
        off_t block_size = COW_BLOCK_SIZE;
        size_t read_ret = raw_device_read((char*)block_buf, 
                                          COW_BLOCK_SIZE, 
                                          block_off); 
        assert(read_ret == COW_BLOCK_SIZE);
        
        /*do cow*/
        ret = do_cow(block_off, block_size, (char*)block_buf, true);
        assert(ret == 0); 
        free(block_buf);

        /*rollback*/
        cow_object_t roll_block_object = roll_block.blk_object();
        void* roll_buf = nullptr;
        ret = posix_memalign((void**)&roll_buf, 512, COW_BLOCK_SIZE);
        assert(ret == 0 && roll_buf != nullptr);
        ret = m_block_store->read(roll_block_object, (char*)roll_buf, 
                                  block_size, 0);
        assert(ret == block_size);
        size_t write_ret = raw_device_write((char*)roll_buf, 
                                             block_size, 
                                             block_off); 
        assert(read_ret == COW_BLOCK_SIZE);
    }

    ClientContext ctx2;
    DeleteReq del_req;
    DeleteAck del_ack;
    del_req.set_vol_name(m_volume_id);
    del_req.set_snap_id(snap_id);
    Status status = m_rpc_stub->Delete(&ctx2, del_req, &del_ack);

    LOG_INFO << "SnapshotProxy do rollback" 
             << " snap_id: " << snap_id << " ok" ;
    return 0;
}

string SnapshotProxy::remote_get_snapshot_name(snapid_t snap_id)
{
    LOG_INFO << "SnapshotProxy get snapshot name" 
             << " snap_id: " << snap_id;
    ClientContext context;
    GetSnapshotNameReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_id(snap_id);
    GetSnapshotNameAck iack;
    Status st = m_rpc_stub->GetSnapshotName(&context, ireq, &iack);
    LOG_INFO << "SnapshotProxy get snapshot name"
             << " snap_id:" << snap_id 
             << " snap_name:" << iack.snap_name()
             << " ok";

    return iack.snap_name();
}

snapid_t SnapshotProxy::remote_get_snapshot_id(string snap_name)
{
    LOG_INFO << "SnapshotProxy get snapshot id" 
             << " snap_name: " << snap_name;
    ClientContext context;
    GetSnapshotIdReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_name(snap_name);
    GetSnapshotIdAck iack;
    Status st = m_rpc_stub->GetSnapshotId(&context, ireq, &iack);
    LOG_INFO << "SnapshotProxy get snapshot id"
             << " snap_name:" << snap_name
             << " snap_id:" << iack.snap_id()
             << " ok";
    return iack.snap_id();
}

string SnapshotProxy::local_get_snapshot_name(snapid_t snap_id)
{
    return m_snapshots_ids[snap_id];
}

snapid_t SnapshotProxy::local_get_snapshot_id(string snap_name)
{
    return m_snapshots[snap_name];
}

int SnapshotProxy::set_status(const uint64_t& snap_id, 
                         const enum huawei::proto::SnapshotStatus& snap_status)
{
    LOG_INFO << "SnapshotProxy set snapshot" 
             << " snap_id: " << snap_id;
    ClientContext context;
    SetStatusReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_id(snap_id);
    ireq.set_snap_status(snap_status);
    SetStatusAck iack;
    Status st = m_rpc_stub->SetStatus(&context, ireq, &iack);
    LOG_INFO << "SnapshotProxy set snapshot status"
             << " snap_id:" << snap_id 
             << " ok";
    return 0;
}

int SnapshotProxy::get_status(const uint64_t& snap_id, 
                              enum huawei::proto::SnapshotStatus& snap_status)
{   

    LOG_INFO << "SnapshotProxy get snapshot" 
             << " snap_id: " << snap_id;
    ClientContext context;
    GetStatusReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_id(snap_id);
    GetStatusAck iack;
    Status st = m_rpc_stub->GetStatus(&context, ireq, &iack);
    snap_status = iack.snap_status();
    LOG_INFO << "SnapshotProxy get snapshot" 
             << " snap_id: " << snap_id
             << " ok";
    return 0;
}

void SnapshotProxy::split_cow_block(const off_t&  off, 
                                    const size_t& size,
                                    vector<cow_block_t>& cow_blocks)
{
    off_t  start = off;
    off_t  end   = off + size;
    size_t len   = size;
    block_t cur_blk_no;
    size_t  split_size;
    while(start < end && len > 0){
       if(start % COW_BLOCK_SIZE  == 0){
            cur_blk_no  = start / COW_BLOCK_SIZE;
            split_size  = min(len,  COW_BLOCK_SIZE);
        } else {
            cur_blk_no  = start / COW_BLOCK_SIZE;
            split_size  = min(len, ((cur_blk_no+1)*COW_BLOCK_SIZE) - start); 
        }
        cow_block_t cow_block;
        cow_block.off    = start;
        cow_block.len    = split_size;
        cow_block.blk_no = cur_blk_no;
        cow_blocks.push_back(cow_block);
        start += split_size;
        len   -= split_size;
    }
}

size_t SnapshotProxy::raw_device_write(char* buf, size_t len, off_t off)
{
    size_t left  = len;
    size_t write = 0;
    while(left > 0){
        int ret = pwrite(m_block_fd, buf+write, left, off+write);
        if(ret == -1 || ret == 0){
             LOG_ERROR << "pwrite fd:" << m_block_fd 
                       << " left:"  << left 
                       << " ret:"   << ret
                       << " errno:" << errno; 
            return ret;
        }
        left  -= ret;
        write += ret;
    }
    return len;
}

size_t SnapshotProxy::raw_device_read(char* buf, size_t len, off_t off)
{
    size_t left = len;
    size_t read = 0;
    while(left > 0){
        int ret = pread(m_block_fd, buf+read, left, off+read);
        if(ret == -1 || ret == 0){
            LOG_ERROR << "pread fd:" << m_block_fd 
                      << " left:" << left 
                      << " ret:"  << ret
                      << " errno:" << errno; 
            return ret;
        } 
        left -= ret;
        read += ret;
    }
    return read;
}

int SnapshotProxy::do_cow(const off_t& off, const size_t& size, char* buf, 
                          bool rollback)
{
    vector<cow_block_t> cow_blocks;
    split_cow_block(off, size, cow_blocks);

    LOG_INFO << "SnapshotProxy do_cow" 
             << " snap_id:" << m_active_snapid
             << " off:"     << off
             << " size:"    << size
             << " rollback:" << rollback;

    for(auto cow_block : cow_blocks){
        LOG_INFO << "SnapshotProxy do_cow" 
                 << " off:" << cow_block.off 
                 << " len:" << cow_block.len 
                 << " blk_no:" << cow_block.blk_no;

        /*todo: maintain bitmap or bloom filter to lookup block cow or overlap*/

        /*get cow meta from dr server*/ 
        ClientContext ctx1;
        Status status;
        CowReq cow_req;
        CowAck cow_ack;
        cow_req.set_vol_name(m_volume_id);
        cow_req.set_snap_id(m_active_snapid);
        cow_req.set_blk_no(cow_block.blk_no);
        status = m_rpc_stub->CowOp(&ctx1, cow_req, &cow_ack);

        /*cow or direct overlap*/
        if(cow_ack.op() == ::huawei::proto::COW_NO){
            LOG_INFO << "SnapshotProxy do overlap"; 
            if(!rollback){
                /*comon io write, overlap*/
                char*   block_buf = buf + cow_block.off - off;
                off_t   block_off = cow_block.off;
                size_t  block_len = cow_block.len;
                size_t  write_ret = raw_device_write(block_buf, 
                                                     block_len, 
                                                     block_off);
                assert(write_ret == block_len);
            } else {
                /*being rollback, do nothing*/ 
            }
            continue;
        } 

        /*read from block device*/
        void* block_buf = nullptr;
        int ret = posix_memalign((void**)&block_buf, 512, COW_BLOCK_SIZE);
        assert(ret == 0 && block_buf != nullptr);
        off_t block_off = cow_block.blk_no * COW_BLOCK_SIZE;
        size_t read_ret = raw_device_read((char*)block_buf, 
                                          COW_BLOCK_SIZE, 
                                          block_off); 
        assert(read_ret == COW_BLOCK_SIZE);

        /*write to cow object*/
        string cow_object = cow_ack.cow_blk_object();
        ret = m_block_store->write(cow_object, (char*)block_buf, 
                                   COW_BLOCK_SIZE, 0);
        assert(ret == 0);
        free(block_buf);

        /*write new data to block device*/
        char*   cow_buf = buf + cow_block.off - off;
        off_t   cow_off = cow_block.off;
        size_t  cow_len = cow_block.len;
        size_t write_ret = raw_device_write(cow_buf, cow_len, cow_off);
        assert(write_ret == cow_len);

        /*update cow meta to dr server*/
        ClientContext ctx2;
        CowUpdateReq update_req;
        CowUpdateAck update_ack;
        update_req.set_vol_name(m_volume_id);
        update_req.set_snap_id(m_active_snapid);
        update_req.set_blk_no(cow_block.blk_no);
        update_req.set_cow_blk_object(cow_object);
        status = m_rpc_stub->CowUpdate(&ctx2, update_req, &update_ack);
    }

    LOG_INFO << "SnapshotProxy do_cow" 
             << " snap_id:" << m_active_snapid
             << " off:"     << off
             << " size:"    << size
             << " rollback:" << rollback
             << " ok";
    return 0;
}

int SnapshotProxy::diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack)
{
    string vname = req->vol_name();
    string first_snap_name = req->first_snap_name();
    string last_snap_name = req->last_snap_name();

    LOG_INFO << "SnapshotProxy diff_snapshot"
             << " vname:" << vname
             << " first_snap:" << first_snap_name
             << " last_snap:"  << last_snap_name;

    ClientContext context;
    DiffReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_first_snap_name(first_snap_name);
    ireq.set_laste_snap_name(last_snap_name);
    DiffAck iack;
    Status st = m_rpc_stub->Diff(&context, ireq, &iack);
    
    int diff_blocks_num = iack.diff_blocks_size();
    for(int i = 0; i < diff_blocks_num; i++){
        ::huawei::proto::DiffBlocks diffblock = iack.diff_blocks(i); 

        ::huawei::proto::ExtDiffBlocks* ext_diff_blocks = ack->add_diff_blocks();
        ext_diff_blocks->set_vol_name(diffblock.vol_name());
        ext_diff_blocks->set_snap_name(diffblock.snap_name());

        LOG_INFO << "SnapshotProxy diff_snapshot"
                 << " snapname:" << diffblock.snap_name();
        int block_num = diffblock.diff_block_no_size(); 
        for(int j = 0; j < block_num; j++){
           uint64_t blk_no = diffblock.diff_block_no(j); 
           LOG_INFO << "SnapshotProxy diff_snapshot" 
                    << " snapname:" << diffblock.snap_name()
                    << " blk_no:" << blk_no;
           ::huawei::proto::ExtDiffBlock* ext_diff_block = ext_diff_blocks->add_diff_block();
           ext_diff_block->set_off(blk_no * COW_BLOCK_SIZE);
           ext_diff_block->set_len(COW_BLOCK_SIZE);
        }
    }
    
    LOG_INFO << "SnapshotProxy diff_snapshot"
             << " vname:" << vname
             << " first_snap:" << first_snap_name
             << " last_snap:"  << last_snap_name
             << " ok";
    return 0; 
}

int SnapshotProxy::read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    off_t  off   = req->off();
    size_t len   = req->len();

    LOG_INFO << "SnapshotProxy read_snapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " off:"   << off
             << " len:"   << len;

    ClientContext ctx0;
    ReadReq ireq;
    ireq.set_vol_name(vname);
    ireq.set_snap_name(sname);
    ireq.set_off(off);
    ireq.set_len(len);
    ReadAck iack;
    Status st = m_rpc_stub->Read(&ctx0, ireq, &iack);
    
    char* read_buf = nullptr;
    int ret = posix_memalign((void**)&read_buf, 512, len);
    assert(ret == 0 && read_buf != nullptr);

    /*--------first read----------------------*/
    interval_set<uint64_t> read_region; 
    read_region.insert(off, len);
    
    /*region read from orginal block device*/
    interval_set<uint64_t> read_device_region;
    read_device_region.insert(off, len);

    /*region read from cow object*/
    interval_set<uint64_t> read_cowobj_region;
    
    /*cow block set in first read*/
    set<uint64_t> cow_block_set0;

    int read_blk_num = iack.read_blocks_size(); 
    for(int i = 0; i < read_blk_num; i++){
        uint64_t block_no = iack.read_blocks(i).blk_no();
        string   block_object = iack.read_blocks(i).blk_object();
        
        /*accumulate record which cow block */
        cow_block_set0.insert(block_no);

        /*accumulate record which read from cow object*/
        read_cowobj_region.insert(block_no * COW_BLOCK_SIZE, COW_BLOCK_SIZE);

        interval_set<uint64_t> block_region;
        block_region.insert(block_no * COW_BLOCK_SIZE, COW_BLOCK_SIZE);
        block_region.intersection_of(read_region);

        /*block region read from cow object*/
        for(interval_set<uint64_t>::iterator it = block_region.begin();
            it != block_region.end(); it++){
            char*  rbuf = read_buf + it.get_start() - off;
            size_t rlen = it.get_len();
            off_t  roff = it.get_start() - (block_no * COW_BLOCK_SIZE);
            LOG_INFO << "SnapshotProxy read_snapshot first read cow object"
                     << " blk_no:" << block_no
                     << " blk_ob:" << block_object
                     << " cow_off:"  << it.get_start()
                     << " cow_len:"  << it.get_len();
            size_t read_ret = m_block_store->read(block_object, rbuf, rlen, roff);
            assert(read_ret == rlen);
        }
    }
    
    /*compute which read from block device*/
    read_device_region.subtract(read_cowobj_region);
    for(interval_set<uint64_t>::iterator it = read_device_region.begin();
        it != read_device_region.end(); it++){
        off_t  r_off = it.get_start();
        size_t r_len = it.get_len();
        char*  r_buf = read_buf + r_off - off;
        LOG_INFO << "SnapshotProxy read_snapshot first read block device"
                 << " cur_off:" << r_off
                 << " cur_len:" << r_len
                 << " align_off:" << ALIGN_UP(r_off, 512)
                 << " align_len:" << ALIGN_UP(r_len, 512);
        size_t r_ret = raw_device_read(r_buf, 
                                       ALIGN_UP(r_len, 512), 
                                       ALIGN_UP(r_off, 512)); 
        assert(r_ret == r_len);
    }

    /*----------second read---------------*/
    ClientContext ctx1;
    ReadReq ireq1;
    ireq1.set_vol_name(vname);
    ireq1.set_snap_name(sname);
    ireq1.set_off(off);
    ireq1.set_len(len);
    ReadAck iack1;
    st = m_rpc_stub->Read(&ctx1, ireq1, &iack1);
    
    int read_block_num1 = iack1.read_blocks_size();
    for(int i = 0; i < read_block_num1; i++){
        uint64_t block_no = iack1.read_blocks(i).blk_no();
        string   block_object = iack1.read_blocks(i).blk_object();
        
        /*cow block has read during first second*/
        if(cow_block_set0.find(block_no) != cow_block_set0.end()){
            continue;
        }
        
        /*when second read, some region in first read from block deivce should
         *read from new snapshot cow object*/
        interval_set<uint64_t> block_region;
        block_region.insert(block_no * COW_BLOCK_SIZE, COW_BLOCK_SIZE);
        block_region.intersection_of(read_device_region);

        /*block region read from cow object*/
        for(interval_set<uint64_t>::iterator it = block_region.begin();
            it != block_region.end(); it++){
            char*  rbuf = read_buf + it.get_start() - off;
            size_t rlen = it.get_len();
            off_t  roff = it.get_start() - (block_no * COW_BLOCK_SIZE);

            LOG_INFO << "SnapshotProxy read_snapshot second read cow object"
                     << " blk_no:" << block_no
                     << " blk_ob:" << block_object
                     << " cow_off:"  << it.get_start()
                     << " cow_len:"  << it.get_len();
 
            m_block_store->read(block_object, rbuf, rlen, roff);
        }
    }
    
    ack->set_ret(0);
    string* data = ack->add_data();
    data->copy(read_buf, len);
    if(read_buf){
        free(read_buf);
    }

    LOG_INFO << "SnapshotProxy read_snapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " off:"   << off
             << " len:"   << len
             << " ok";
    return 0; 
}
