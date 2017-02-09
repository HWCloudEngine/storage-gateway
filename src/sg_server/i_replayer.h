/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    i_replayer.h
* Author: 
* Date:         2017/01/23
* Version:      1.0
* Description:
* 
************************************************/
#ifndef I_REPLAYER_H_
#define I_REPLAYER_H_
#include <memory>
#include "consumer_interface.h"
#include "journal_meta_manager.h"
#include "../rpc/consumer.pb.h"
using huawei::proto::DRS_OK;
using huawei::proto::REPLAYER;
class IReplayer:public IConsumer {
    std::shared_ptr<JournalMetaManager> meta_;
public:
    IReplayer(const std::string& vol_id,
            std::shared_ptr<JournalMetaManager> meta):
            IConsumer(vol_id),
            meta_(meta){
    }
    ~IReplayer(){
    }

    virtual CONSUMER_TYPE get_type(){
        return REPLAYER;
    }

    virtual int get_consumer_marker(JournalMarker& marker){
        if(DRS_OK == meta_->get_consumer_marker(vol_,REPLAYER,marker))
            return 0;
        else
            return -1;
    }

    virtual int update_consumer_marker(const JournalMarker& marker){
        if(DRS_OK == meta_->update_consumer_marker(vol_,REPLAYER,marker))
            return 0;
        else
            return -1;
    }

    virtual int get_consumable_journals(const JournalMarker& marker,
            const int limit, std::list<JournalElement>& list){
        if(DRS_OK == meta_->get_consumable_journals(vol_,marker,limit,list,REPLAYER))
            return 0;
        else
            return -1;
    }
};
#endif
