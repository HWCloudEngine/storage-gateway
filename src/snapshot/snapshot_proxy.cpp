#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <set>
#include <chrono>
#include "../common/interval_set.h"
#include "../rpc/message.pb.h"
#include "snapshot_proxy.h"

using huawei::proto::CreateReq;
using huawei::proto::CreateAck;
using huawei::proto::ListReq;
using huawei::proto::ListAck;
using huawei::proto::DeleteReq;
using huawei::proto::DeleteAck;
using huawei::proto::CowReq;
using huawei::proto::CowAck;
using huawei::proto::UpdateReq;
using huawei::proto::UpdateAck;
using huawei::proto::CowUpdateReq;
using huawei::proto::CowUpdateAck;
using huawei::proto::RollbackReq;
using huawei::proto::RollbackAck;
using huawei::proto::DiffReq;
using huawei::proto::DiffAck;
using huawei::proto::ReadReq;
using huawei::proto::ReadAck;
using huawei::proto::SyncReq;
using huawei::proto::SyncAck;
using huawei::proto::SnapshotMessage;

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
    
    m_snapshots.clear();
    m_active_snapshot.clear();
    m_exist_snapshot = false;
    
    /*sync with dr server*/
    sync_state();

    return true;
}

bool SnapshotProxy::fini()
{
    m_snapshots.clear();
    m_active_snapshot.clear();
    m_exist_snapshot = false;
 
    if(m_block_store){
        delete m_block_store;
    }

    if(m_block_fd != -1){
        close(m_block_fd); 
    }

    return true;
}

int SnapshotProxy::sync_state()
{
    LOG_INFO << "SnapshotProxy sync_state";

    ClientContext context;
    SyncReq ireq;
    ireq.set_vol_name(m_volume_id);
    SyncAck iack;
    Status st = m_rpc_stub->Sync(&context, ireq, &iack);
    if(!st.ok()) {
        LOG_INFO << "SnapshotProxy sync_state failed";
        return -1;
    }

    m_active_snapshot = iack.latest_snap_name();
    if(!m_active_snapshot.empty()){
        m_exist_snapshot = true; 
    }

    LOG_INFO << "SnapshotProxy sync_state" << " ret:" << iack.ret();
    return 0;
}

int SnapshotProxy::cmd_persist_wait()
{
    unique_lock<std::mutex> ulock(m_cmd_persit_lock);
    m_cmd_persit_cond.wait(ulock);
    return 0;
}

void SnapshotProxy::cmd_persist_notify()
{
    unique_lock<std::mutex> ulock(m_cmd_persit_lock);
    m_cmd_persit_cond.notify_all();
}

int SnapshotProxy::create_snapshot(const CreateSnapshotReq* req, 
                                   CreateSnapshotAck* ack)
{
    /*get from exterior rpc*/
    string vname = req->vol_name();
    string sname = req->snap_name();
    
    LOG_INFO << "SnapshotProxy create_snapshot"
             << " vname:" << vname << " sname:" << sname;

    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = spawn_journal_entry(sname, SNAPSHOT_CREATE); 
    
    /*push journal entry to entry queue*/
    m_entry_queue.push(entry);
    
    int ret = 0;
    snapshot_status_t cur_snap_status = SNAPSHOT_CREATING;
    m_snapshots.insert(pair<string, snapshot_status_t>(sname, cur_snap_status)) ;
    do {
        /*todo: wait journal writer persist journal entry ok and ack*/
        ret = cmd_persist_wait();
        if(ret){
            LOG_INFO << "SnapshotProxy create_snapshot"
                     << " vname:" << vname << " sname:" << sname
                     << " failed persist timeout";
            break;
        }

        /*rpc with dr_server */
        ret = do_create(sname);
        if(ret){
            LOG_INFO << "SnapshotProxy create_snapshot"
                     << " vname:" << vname << " sname:" << sname
                     << " failed rpc error";
            break;
        }

        LOG_INFO << "SnapshotProxy create_snapshot"
                 << " vname:" << vname << " sname:" << sname
                 << " ok";
    }while(0);
    
    m_snapshots.erase(sname);

    return ret;
}

int SnapshotProxy::list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack)
{
    string vname = req->vol_name();
    LOG_INFO << "SnapshotProxy list_snapshot" << " vname:" << vname;

    ClientContext context;
    ListReq ireq;
    ireq.set_vol_name(vname);
    ListAck iack;
    Status st = m_rpc_stub->List(&context, ireq, &iack);
    int snap_num = iack.snap_name_size();
    for(int i = 0; i < snap_num; i++){
       ack->add_snap_name(iack.snap_name(i)); 
    }
    LOG_INFO << "SnapshotProxy list_snapshot" << " vname:" << vname
             << " snap_size:" << iack.snap_name_size() << " ok";
    return 0; 
}

int SnapshotProxy::delete_snapshot(const DeleteSnapshotReq* req, 
                                   DeleteSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "SnapshotProxy delete_snapshot" << " vname:" << vname
             << " sname:" << sname;

    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = spawn_journal_entry(sname, SNAPSHOT_DELETE);
   
    /*push journal entry to write queue*/
    m_entry_queue.push(entry);
    
    int ret = 0;
    snapshot_status_t cur_snap_status = SNAPSHOT_DELETING;
    m_snapshots.insert(pair<string, snapshot_status_t>(sname, cur_snap_status)) ;
    do {
        /*wait journal writer persist journal entry ok and ack*/
        ret = cmd_persist_wait();
        if(ret){
            LOG_INFO << "SnapshotProxy delete_snapshot"
                     << " vname:" << vname 
                     << " sname:" << sname
                     << " failed persist timeout";
            break;
        }
        /*rpc with dr_server */
        ret = do_delete(sname);
        if(ret){
            LOG_INFO << "SnapshotProxy delete_snapshot"
                     << " vname:" << vname 
                     << " sname:" << sname
                     << " failed rpc error";
            break;
        }

        LOG_INFO << "SnapshotProxy delete_snapshot"
                 << " vname:" << vname 
                 << " sname:" << sname
                 << " ok";
    }while(0);
    
    m_snapshots.erase(sname);
    return ret;
}

int SnapshotProxy::rollback_snapshot(const RollbackSnapshotReq* req, 
                                     RollbackSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();

    LOG_INFO << "SnapshotProxy rollback_snapshot"
             << " vname:" << vname
             << " sname:" << sname;

    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = spawn_journal_entry(sname, SNAPSHOT_ROLLBACK);
    
    /*push journal entry to write queue*/
    m_entry_queue.push(entry);
    
    int ret = 0;
    snapshot_status_t cur_snap_status = SNAPSHOT_ROLLBACKING;
    m_snapshots.insert(pair<string, snapshot_status_t>(sname, cur_snap_status)) ;
    do {
        /*wait journal writer persist journal entry ok and ack*/
        ret = cmd_persist_wait();
        if(ret){
            LOG_INFO << "SnapshotProxy rollback_snapshot"
                     << " vname:" << vname 
                     << " sname:" << sname
                     << " failed persist timeout";
            break;
        }

        ret = do_rollback(sname);
        if(ret){
            LOG_INFO << "SnapshotProxy rollback_snapshot"
                     << " vname:" << vname 
                     << " sname:" << sname
                     << " failed rpc error";
            break;
        }

        LOG_INFO << "SnapshotProxy rollback_snapshot"
                 << " vname:" << vname 
                 << " sname:" << sname
                 << " ok";
    }while(0);
    
    m_snapshots.erase(sname);
    return ret;
}

int SnapshotProxy::create_transaction(string snap_name)
{
    LOG_INFO << "create transaction begin";
    int ret = transaction(snap_name);
    if(ret){
        LOG_ERROR << "create transaction failed";
        return ret;
    }

    m_active_snapshot = snap_name;
    m_exist_snapshot  = true;
    LOG_INFO << "create transaction end";
    return ret;
}

int SnapshotProxy::delete_transaction(string snap_name)
{
    LOG_INFO << "delete transaction begin";
    int ret = transaction(snap_name);
    if(ret){
        LOG_ERROR << "delete transaction failed";
        return ret;
    }
    
    if(snap_name.compare(m_active_snapshot) == 0){
        m_active_snapshot.clear();
        m_exist_snapshot = false;
    }

    LOG_INFO << "delete transaction end";
    return ret;
}

int SnapshotProxy::rollback_transaction(string snap_name)
{
    LOG_INFO << "rollback transaction begin";
    int ret = transaction(snap_name);
    if(ret){
        LOG_ERROR << "rollback transaction failed";
        return ret;
    }

    ClientContext context;
    RollbackReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_name(snap_name);
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
    
    /*dr server to delete rollback snapshot*/
    ret = do_update(snap_name);

    if(m_active_snapshot.compare(snap_name) == 0){
        m_exist_snapshot  = false;
    }

    LOG_INFO << "rollback transaction end";
    return ret;
}

shared_ptr<JournalEntry> SnapshotProxy::spawn_journal_entry(string snap_name,
                                                    journal_event_type_t type)
{
    /*spawn snapshot message*/
    shared_ptr<SnapshotMessage> message = make_shared<SnapshotMessage>();
    message->set_snap_name(snap_name); 

    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = make_shared<JournalEntry>();
    entry->set_type(type);
    entry->set_message(message);
    return entry; 
}

int SnapshotProxy::transaction(string snap_name) 
{
    int ret = 0;
    auto it = m_snapshots.find(snap_name);

    while(it != m_snapshots.end())
    {
        usleep(200);
        it = m_snapshots.find(snap_name);
    }        

    /*trigger dr server update snapshot status*/
    ret = do_update(snap_name);

    return ret;
}

bool SnapshotProxy::check_exist_snapshot()const
{
    return (!m_active_snapshot.empty() && m_exist_snapshot) ? true : false;
}

int SnapshotProxy::do_create(string snap_name)
{
    LOG_INFO << "SnapshotProxy do_create" << " snap_name:" << snap_name;

    ClientContext context;
    CreateReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_name(snap_name);
    CreateAck iack;
    Status st = m_rpc_stub->Create(&context, ireq, &iack);
        
    LOG_INFO << "SnapshotProxy do_create" << " snap_name:" << snap_name
             << " ret:"   << iack.ret();
    return 0;
}

int SnapshotProxy::do_delete(string snap_name)
{
    LOG_INFO << "SnapshotProxy do_delete"
             << " snap_name:" << snap_name;
    
    /*really tell dr server delete snapshot*/
    ClientContext context;
    DeleteReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_name(snap_name);
    DeleteAck iack;
    Status st = m_rpc_stub->Delete(&context, ireq, &iack);

    LOG_INFO << "SnapshotProxy do_delete"
             << " snap_name:" << snap_name
             << " ret:"       << iack.ret();
    return 0;
}

int SnapshotProxy::do_rollback(string snap_name)
{
    return 0;
}

int SnapshotProxy::do_update(const string& snap_name)
{
    LOG_INFO << "SnapshotProxy do_update" << " snap_name: " << snap_name;
    ClientContext context;
    UpdateReq ireq;
    ireq.set_vol_name(m_volume_id);
    ireq.set_snap_name(snap_name);
    UpdateAck iack;
    Status st = m_rpc_stub->Update(&context, ireq, &iack);
    if(!st.ok()){
        LOG_INFO << "SnapshotProxy do_update" << " snap_name:" 
                 << snap_name << " failed";
        return -1;
    }

    LOG_INFO << "SnapshotProxy do_update" << " snap_name:" << snap_name 
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
             << " snap_id:" << m_active_snapshot
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
        cow_req.set_snap_name(m_active_snapshot);
        cow_req.set_blk_no(cow_block.blk_no);
        status = m_rpc_stub->CowOp(&ctx1, cow_req, &cow_ack);

        /*cow or direct overlap*/
        if(cow_ack.op() == COW_NO){
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
        update_req.set_snap_name(m_active_snapshot);
        update_req.set_blk_no(cow_block.blk_no);
        update_req.set_cow_blk_object(cow_object);
        status = m_rpc_stub->CowUpdate(&ctx2, update_req, &update_ack);
    }

    LOG_INFO << "SnapshotProxy do_cow" 
             << " snap_name:" << m_active_snapshot
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

    LOG_INFO << "SnapshotProxy diff_snapshot" << " vname:" << vname
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

        ExtDiffBlocks* ext_diff_blocks = ack->add_diff_blocks();
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
           ExtDiffBlock* ext_diff_block = ext_diff_blocks->add_diff_block();
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

    LOG_INFO << "SnapshotProxy read_snapshot" << " vname:" << vname
             << " sname:" << sname << " off:" << off << " len:" << len;

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

    LOG_INFO << "SnapshotProxy read_snapshot" << " vname:" << vname
             << " sname:" << sname << " off:" << off << " len:" << len << " ok";
    return 0; 
}
