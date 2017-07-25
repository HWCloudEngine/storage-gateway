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
#ifndef STORAGE_GATEWAY_RPC_CLIENT_H__
#define STORAGE_GATEWAY_RPC_CLIENT_H__
#include <functional>
#include <thread>
#include <chrono>
#include <grpc++/grpc++.h>
#include "log/log.h"
#include "writer_client.h"
#include "lease_rpc_client.h"
#include "replayer_client.h"
#include "replicate_inner_ctrl_client.h"
#include "volume_inner_ctrl_client.h"
#include "backup_rpccli.h"
#include "snapshot_rpccli.h"
#include "common/config_option.h"
#include "common/utils.h"

void rpc_init();

template <class> class Decorator;
template <class R, class... Args>
class Decorator<R(Args ...)>
{
public:
    Decorator(){}
    Decorator(std::function<R(Args ...)> f):f_(f){}

    R operator()(Args ... args)
    {
        //TODO more policy,read config option
        int max_attemps = 3;
        int i = 0;
        StatusCode status;
        do
        {
            for(;i < max_attemps;i++)
            {
                status = f_(args...);
                if(status == StatusCode::sOk)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            //restart all clients channel and stub
            if (status != StatusCode::sOk)
            {
                rpc_init();
                //try again
                status = f_(args...);
            }
        }
        while(status != StatusCode::sOk && i < max_attemps);
        return status;
    }

    Decorator<R(Args ...)>& operator=(const Decorator<R(Args ...)>& f)
    {
        f_ = f.f_;
        return *this;
    }

    std::function<R(Args ...)> f_;
};        


template<class R, class... Args>
Decorator<R(Args ...)> make_decorator(R (*f)(Args ...))
{
    return Decorator<R(Args...)>(std::function<R(Args...)>(f));
}


class RpcClient
{
public:
    RpcClient();
    static RpcClient& instance();
    void register_func();
    
public:
    //writer_client
    Decorator<StatusCode(const std::string&,
        const std::string&, const int,std::list<JournalElement>&)> GetWriteableJournals;
    Decorator<StatusCode(const std::string&,
        const std::string&, const std::list<std::string>&)> SealJournals;
    Decorator<StatusCode(const std::string&,
            const std::string&, const JournalMarker&)> update_producer_marker;
    Decorator<StatusCode(const std::string&,
            const std::map<std::string,JournalMarker>&)> update_multi_producer_markers;

    //lease rpc client
    Decorator<StatusCode(const std::string&,
                         const std::string&,
                         std::map<std::string,std::string>&)> update_lease;

    //replayer_client
    Decorator<StatusCode(const std::string&,JournalMarker&)> GetJournalMarker;
    Decorator<StatusCode(const std::string&,const JournalMarker&,
        int,std::list<JournalElement>&)> GetJournalList;
    Decorator<StatusCode(const JournalMarker&,const std::string&)> UpdateConsumerMarker;

    //replicate_inner_ctrl_client
    Decorator<StatusCode(const string&,const string&,const string&,
        const std::list<string>&,const RepRole&)> create_replication;
    Decorator<StatusCode(const string&,const string&,const RepRole&,
        const JournalMarker&,const string&)> enable_replication;
    Decorator<StatusCode(const string&,const string&,const RepRole&,
        const JournalMarker&,const string&)> disable_replication;
    Decorator<StatusCode(const string&,const string&,const RepRole&,
        const JournalMarker&,const bool&,const string&)> failover_replication;
    Decorator<StatusCode(const string&,const string&,const RepRole&)> reverse_replication;
    Decorator<StatusCode(const string&,const string&,const RepRole&)> delete_replication;
    Decorator<StatusCode(const string&,const string&,const RepRole&, bool&)> report_checkpoint;

    //volume_inner_ctrl_client
    Decorator<StatusCode(const std::string&,const std::string&,
        const uint64_t&, const VolumeStatus&)> create_volume;
    Decorator<StatusCode(const std::string&,VolumeInfo&)> get_volume;
    Decorator<StatusCode(std::list<VolumeInfo>&)> list_volume;
    Decorator<StatusCode(const std::string&)> delete_volume;
    Decorator<StatusCode(const UpdateVolumeReq&)> update_volume;

    //backup rpc client
    Decorator<StatusCode(const std::string&, const size_t&,const std::string&,
         const BackupOption&)> CreateBackup;
    Decorator<StatusCode(const std::string&, std::set<std::string>&)> ListBackup;
    Decorator<StatusCode(const std::string&, const std::string&,
         BackupStatus&)> GetBackup;
    Decorator<StatusCode(const std::string&, const std::string&)> DeleteBackup;
    Decorator<StatusCode(const std::string&,const std::string&, const BackupType&,
         const std::string&,const size_t&,const std::string&,BlockStore*)> RestoreBackup;

    //snapshot rpc client
    Decorator<StatusCode(const std::string&, std::string&)> do_init_sync;
    Decorator<StatusCode(const SnapReqHead&,const std::string&,
                         const std::string&)> do_create;
    Decorator<StatusCode(const SnapReqHead&,const std::string&,const std::string&)> do_delete;
    Decorator<StatusCode(const SnapReqHead&,const std::string&, const std::string&,
                         std::vector<RollBlock>&)> do_rollback;
    Decorator<StatusCode(const SnapReqHead&, const std::string&,const std::string&,
                         const UpdateEvent&,std::string&)> do_update;
    Decorator<StatusCode(const std::string&,std::set<std::string>&)> do_list;
    Decorator<StatusCode(const std::string&,const std::string,SnapStatus&)> do_query;
    Decorator<StatusCode(const SnapReqHead&,const std::string&,const std::string&,
                         const std::string&,std::vector<DiffBlocks>&)> do_diff;
    Decorator<StatusCode(const SnapReqHead&,const std::string&,const std::string&, const off_t,
                         const size_t,std::vector<ReadBlock>&)> do_read;
    Decorator<StatusCode(const std::string&,const std::string&,const uint64_t,
                         cow_op_t&,std::string&)> do_cow_check;
    Decorator<StatusCode(const std::string&,const std::string&,const uint64_t,
                         const bool,const std::string)> do_cow_update;
};

#define g_rpc_client (RpcClient::instance())

#endif
