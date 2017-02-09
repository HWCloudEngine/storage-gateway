/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    consumer_interface.h
* Author: 
* Date:         2017/01/23
* Version:      1.0
* Description:
* 
************************************************/
#ifndef CONSUMER_INTERFACE_H_
#define CONSUMER_INTERFACE_H_
#include <list>
#include <string>
#include "../rpc/consumer.pb.h"
using huawei::proto::JournalElement;
using huawei::proto::CONSUMER_TYPE;
class IConsumer {
protected:
    std::string vol_;
public:
    IConsumer(const std::string& vol_id):
            vol_(vol_id){
    }
    ~IConsumer(){}
    virtual std::string get_vol_id(){
        return vol_;
    }
    virtual CONSUMER_TYPE get_type() = 0;
    virtual int get_consumer_marker(JournalMarker& marker) = 0;
    virtual int update_consumer_marker(const JournalMarker& marker) = 0;
    virtual int get_consumable_journals(const JournalMarker& marker,
            const int limit, std::list<JournalElement>& list) = 0;
    virtual void consume(){}
};
#endif
