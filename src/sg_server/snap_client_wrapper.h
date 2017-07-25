/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    snap_client_wrapper.h
* Author: 
* Date:         2017/02/20
* Version:      1.0
* Description:  singleton snap reader wrapped a SnapshotCtrlClient
* 
************************************************/
#ifndef SNAP_CLIENT_WRAPPER_H_
#define SNAP_CLIENT_WRAPPER_H_
#include <memory>
#include "rpc/clients/snapshot_ctrl_client.h"
#include "log/log.h"
#include "common/config_option.h"
#include "common/observe.h"
#include "common/utils.h"
#include "volume_inner_control.h"
using huawei::proto::VolumeInfo;

class SnapClientWrapper : public Observee {
    std::shared_ptr<SnapshotCtrlClient> snap_ctrl_client;
public:
    static SnapClientWrapper& instance(){
        static SnapClientWrapper reader;
        return reader;
    }

    SnapClientWrapper(SnapClientWrapper&) = delete;
    SnapClientWrapper& operator=(SnapClientWrapper const&) = delete;

    std::shared_ptr<SnapshotCtrlClient> get_client(){
        return snap_ctrl_client;
    }

    bool diff_snapshot_is_empty(const string& vol,const string& cur_snap,
        const string& pre_snap){
        std::vector<DiffBlocks> diff_blocks;
        StatusCode ret = snap_ctrl_client->DiffSnapshot(
                vol,pre_snap,cur_snap,diff_blocks);
        SG_ASSERT(ret == StatusCode::sOk);
        LOG_DEBUG << diff_blocks.size() << " blocks in diffSnapTask.";
        if(diff_blocks.empty()){
            LOG_INFO << "there was no diff blocks in task[" << cur_snap << "]";
            return true;
        }
        else{
            for(int i=0; i<diff_blocks.size(); i++){
                if(diff_blocks[i].block_size() > 0){
                    return false;
                }
            }
            LOG_INFO << "there was no diff block in snap[" << cur_snap
                    << "]";
            return true;
        }
        return false;
    }

    void update(int event, void* args) {
        if (event != UPDATE_VOLUME) {
            LOG_ERROR << "update event:" << event << " not support";
            return;
        }
        VolumeInfo* vol = reinterpret_cast<VolumeInfo*>(args);
        if (vol == nullptr) {
            LOG_ERROR << "update null ptr";
            return;
        }
        std::string m_snap_client_ip = vol->attached_host();
        short m_snap_client_port = g_option.ctrl_server_port;
        if (!network_reachable(m_snap_client_ip.c_str(), m_snap_client_port)) {
            LOG_ERROR << "update ip:" << m_snap_client_ip << " port:" << m_snap_client_port << " unreachable";
            return;
        }
        std::string rpc_addr = rpc_address(m_snap_client_ip, m_snap_client_port);
        snap_ctrl_client.reset(new SnapshotCtrlClient(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials())));
    }

private:
    SnapClientWrapper(){
        std::string rpc_addr = rpc_address(g_option.ctrl_server_ip, g_option.ctrl_server_port);
        snap_ctrl_client.reset(new SnapshotCtrlClient(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials())));
        g_vol_ctrl->add_observee(reinterpret_cast<Observee*>(this));
    }
    ~SnapClientWrapper(){
        g_vol_ctrl->del_observee(reinterpret_cast<Observee*>(this));
    }
};

#endif
