/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    snapshot.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot interface
*  
*************************************************/
#ifndef SRC_SG_CLIENT_SNAPSHOT_SYNCBARRIER_H_
#define SRC_SG_CLIENT_SNAPSHOT_SYNCBARRIER_H_
#include <string>
#include <map>

class ISyncBarrier {
 public:
    ISyncBarrier() = default;
    virtual ~ISyncBarrier() {
    }

    virtual void add_sync(const std::string& actor,
                          const std::string& action) =  0;
    virtual void del_sync(const std::string& actor) = 0;

    /*true: action still on; false: action already over*/
    virtual bool check_sync_on(const std::string& actor) = 0;
};

#endif  // SRC_SG_CLIENT_SNAPSHOT_SYNCBARRIER_H_
