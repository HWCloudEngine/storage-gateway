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
#include "common/config_option.h"
#include "common/utils.h"

using namespace std::placeholders;

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

};

#define g_rpc_client (RpcClient::instance())

#endif
