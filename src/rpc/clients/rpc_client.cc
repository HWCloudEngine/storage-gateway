/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   rpc_client.h 
*  Author: 
*  Date:         2017/06/28
*  Version:      1.0
*  Description:  handle writer io
*
*************************************************/
#include "rpc_client.h"

std::unique_ptr<Writer::Stub> WriterClient::stub_;
std::unique_ptr<Lease::Stub> LeaseRpcClient::stub_;
std::unique_ptr<Consumer::Stub> ReplayerClient::stub_;
CONSUMER_TYPE ReplayerClient::consumer_type;
std::string ReplayerClient::lease_uuid;
std::unique_ptr<ReplicateInnerControl::Stub> RepInnerCtrlClient::stub_;
std::unique_ptr<VolumeInnerControl::Stub> VolInnerCtrlClient::stub_;
std::unique_ptr<BackupInnerControl::Stub> BackupRpcCli::m_stub;
std::shared_ptr<Channel> SnapRpcCli::channel_;
std::unique_ptr<SnapshotInnerControl::Stub> SnapRpcCli::stub_;
Status SnapRpcCli::status_;


void rpc_init(){
    std::string meta_rpc_addr = rpc_address(g_option.meta_server_ip, g_option.meta_server_port);
    std::shared_ptr<Channel> channel = grpc::CreateChannel(meta_rpc_addr,
                    grpc::InsecureChannelCredentials());
    WriterClient::init(channel);
    LeaseRpcClient::init(channel);
    ReplayerClient::init(channel);
    RepInnerCtrlClient::init(channel);
    VolInnerCtrlClient::init(channel);
    BackupRpcCli::init(channel);
    SnapRpcCli::init(channel);
}

RpcClient::RpcClient(){
    rpc_init();
    register_func();
}

RpcClient& RpcClient::instance(){
    static RpcClient rpc_client;
    return rpc_client;
}

void RpcClient::register_func()
{
    //WriterClient
    GetWriteableJournals = make_decorator(&WriterClient::GetWriteableJournals);
    SealJournals = make_decorator(&WriterClient::SealJournals);
    update_producer_marker = make_decorator(&WriterClient::update_producer_marker);
    update_multi_producer_markers = make_decorator(&WriterClient::update_multi_producer_markers);

    //Lease rpc client
    update_lease = make_decorator(&LeaseRpcClient::update_lease);

    //replayer_client
    GetJournalMarker = make_decorator(&ReplayerClient::GetJournalMarker);
    GetJournalList = make_decorator(&ReplayerClient::GetJournalList);
    UpdateConsumerMarker = make_decorator(&ReplayerClient::UpdateConsumerMarker);

    //replicate_inner_ctrl_client
    create_replication = make_decorator(&RepInnerCtrlClient::create_replication);
    enable_replication = make_decorator(&RepInnerCtrlClient::enable_replication);
    disable_replication = make_decorator(&RepInnerCtrlClient::disable_replication);
    failover_replication = make_decorator(&RepInnerCtrlClient::failover_replication);
    reverse_replication = make_decorator(&RepInnerCtrlClient::reverse_replication);
    delete_replication = make_decorator(&RepInnerCtrlClient::delete_replication);
    report_checkpoint = make_decorator(&RepInnerCtrlClient::report_checkpoint);

    //volume_inner_ctrl_client
    create_volume = make_decorator(&VolInnerCtrlClient::create_volume);
    update_volume_status = make_decorator(&VolInnerCtrlClient::update_volume_status);
    get_volume = make_decorator(&VolInnerCtrlClient::get_volume);
    list_volume = make_decorator(&VolInnerCtrlClient::list_volume);
    delete_volume = make_decorator(&VolInnerCtrlClient::delete_volume);
    update_volume_path = make_decorator(&VolInnerCtrlClient::update_volume_path);

    //backup rpc client
    CreateBackup = make_decorator(&BackupRpcCli::CreateBackup);
    ListBackup = make_decorator(&BackupRpcCli::ListBackup);
    GetBackup = make_decorator(&BackupRpcCli::GetBackup);
    DeleteBackup = make_decorator(&BackupRpcCli::DeleteBackup);
    RestoreBackup = make_decorator(&BackupRpcCli::RestoreBackup);

    //snapshot rpc client
    do_init_sync = make_decorator(&SnapRpcCli::do_init_sync);
    do_create = make_decorator(&SnapRpcCli::do_create);
    do_delete = make_decorator(&SnapRpcCli::do_delete);
    do_rollback = make_decorator(&SnapRpcCli::do_rollback);
    do_update = make_decorator(&SnapRpcCli::do_update);
    do_list = make_decorator(&SnapRpcCli::do_list);
    do_query = make_decorator(&SnapRpcCli::do_query);
    do_diff = make_decorator(&SnapRpcCli::do_diff);
    do_read = make_decorator(&SnapRpcCli::do_read);
    do_cow_check = make_decorator(&SnapRpcCli::do_cow_check);
    do_cow_update = make_decorator(&SnapRpcCli::do_cow_update);
}


