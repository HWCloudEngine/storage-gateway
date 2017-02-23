#include "replicate_proxy.h"
#include "../log/log.h"
#include "../common/config_parser.h"
ReplicateProxy::ReplicateProxy(const string& vol_name,
            std::shared_ptr<SnapshotProxy> snapshot_proxy):
            vol_name_(vol_name),
            snapshot_proxy_(snapshot_proxy){
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    string default_ip("127.0.0.1");
    string svr_ip = parser->get_default("meta_server.ip",default_ip);
    int svr_port = parser->get_default("meta_server.port",50051);
    svr_ip += ":" + std::to_string(svr_port);
    rep_inner_client_.reset(new RepInnerCtrlClient(grpc::CreateChannel(svr_ip,
                grpc::InsecureChannelCredentials())));
    vol_inner_client_.reset(new VolInnerCtrlClient(grpc::CreateChannel(svr_ip,
                grpc::InsecureChannelCredentials())));
}

ReplicateProxy::~ReplicateProxy(){
}

StatusCode ReplicateProxy::create_snapshot(const string& snap_name,
        JournalMarker& marker){
    // TODO:
    return StatusCode::sOk;
}

StatusCode ReplicateProxy::create_transaction(const SnapReqHead& shead,
        const string& snap_name){
    StatusCode ret_code;

    // get volume role(primary or secondary)
    VolumeInfo info;
    ret_code = vol_inner_client_->get_volume(vol_name_,info);
    if(ret_code != StatusCode::sOk){
        LOG_ERROR << "get volume failed, volume:" << vol_name_;
        return ret_code;
    }

    // report sg_server that replayer got a replicate snap;
    // sg_server will validate this snap
    RepRole role = info.role();
    bool is_discard;
    ret_code = rep_inner_client_->report_checkpoint(snap_name,
        vol_name_,role,is_discard);
    if(ret_code != StatusCode::sOk){
        LOG_ERROR << "report replicate checkpoint failed, volume:" << vol_name_;
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
        ret_code = snapshot_proxy_->do_update(shead, snap_name,
                UpdateEvent::DELETE_EVENT);
        if(ret_code != StatusCode::sOk){
            LOG_ERROR << "try to delete volume[" << vol_name_
                << "] replicate snapshot[" << snap_name << "]failed!";
        }
    }
    return ret_code;
}

