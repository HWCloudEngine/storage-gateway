#include <thread>
#include <chrono>
#include "log/log.h"
#include "common/utils.h"
#include "common/config_option.h"
#include "replicate_proxy.h"

ReplicateProxy::ReplicateProxy(const string& vol_name,
            const size_t& vol_size,
            std::shared_ptr<SnapshotProxy> snapshot_proxy):
            vol_name_(vol_name),
            vol_size_(vol_size),
            snapshot_proxy_(snapshot_proxy) {

    std::string rpc_addr = rpc_address(g_option.meta_server_ip,
                                       g_option.meta_server_port);
    rep_inner_client_.reset(new RepInnerCtrlClient(grpc::CreateChannel(
                rpc_addr, grpc::InsecureChannelCredentials())));
}

ReplicateProxy::~ReplicateProxy() {
}

StatusCode ReplicateProxy::create_snapshot(const string& snap_name,
        JournalMarker& marker,const string& checkpoint_id,
        const SnapScene& snap_scene,const SnapType& snap_type) {
    CreateSnapshotReq req;
    CreateSnapshotAck ack;
    req.mutable_header()->set_seq_id(0);
    req.mutable_header()->set_scene(snap_scene);
    req.mutable_header()->set_snap_type(snap_type);
    req.mutable_header()->set_replication_uuid("");
    req.mutable_header()->set_checkpoint_uuid(checkpoint_id);
    req.set_vol_name(vol_name_);
    req.set_vol_size(vol_size_);
    req.set_snap_name(snap_name);

    return snapshot_proxy_->create_snapshot(&req, &ack,marker);
}

StatusCode ReplicateProxy::create_transaction(const SnapReqHead& shead,
        const string& snap_name, const RepRole& role) {
    StatusCode ret_code;

    LOG_DEBUG << "replay a replicate snapshot:" << snap_name;
    while (is_sync_item_exist(snap_name)) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    };
    // report sg_server that replayer got a replicate snap;
    // sg_server will validate this snapshot
    bool is_discard;
    ret_code = rep_inner_client_->report_checkpoint(shead.checkpoint_uuid(),
        vol_name_,role,is_discard);
    if(ret_code != StatusCode::sOk){
        LOG_ERROR << "report replicate checkpoint failed, volume:" << vol_name_
            << ",operation id:" << shead.checkpoint_uuid();
        return ret_code;
    }
    if(!is_discard){
        ret_code = snapshot_proxy_->create_transaction(shead, snap_name);
        if(ret_code != StatusCode::sOk){
            LOG_ERROR << "snapshot proxy create transaction failed:" << ret_code;
        }
    }
    else{
        //if snapshot is not found in volume replicate operation records,delete it
        ret_code = snapshot_proxy_->update(shead, snap_name, UpdateEvent::DELETE_EVENT);
        if(ret_code != StatusCode::sOk){
            LOG_ERROR << "try to delete volume[" << vol_name_
                << "] replicate snapshot[" << snap_name << "]failed!";
        }
    }
    return ret_code;
}

void ReplicateProxy::add_sync_item(const std::string& actor,const std::string& action){
    WriteLock write_lock(map_mtx_);
    sync_map_.insert(std::pair<string,string>(actor,action));
}

void ReplicateProxy::delete_sync_item(const std::string& actor){
    WriteLock write_lock(map_mtx_);
    sync_map_.erase(actor);
}

bool ReplicateProxy::is_sync_item_exist(const std::string& actor){
    ReadLock read_lock(map_mtx_);
    auto it = sync_map_.find(actor);
    return (it != sync_map_.end());
}

