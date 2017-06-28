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
class SnapClientWrapper{
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

    bool snapshot_is_empty(const string& vol,const string& snap){
        return false;
    }


private:
    SnapClientWrapper(){
        std::string host = g_option.ctrl_server_ip;
        host += ":" + std::to_string(g_option.ctrl_server_port);
        snap_ctrl_client.reset(new SnapshotCtrlClient(
                grpc::CreateChannel(host, 
                    grpc::InsecureChannelCredentials())));
    }
    ~SnapClientWrapper(){}
};
#endif
