/*
 * lease_manager.h
 *
 *  Created on: 2016Äê8ÔÂ12ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_LEASE_H_
#define JOURNAL_LEASE_H_

#include <string>
#include <list>
#include "rpc/common.pb.h"

using huawei::proto::RESULT;

class JournalLease {
public:
    virtual ~JournalLease() {}
    virtual bool acquire_lease() = 0;
    virtual void renew_lease() = 0;
    virtual bool check_lease_validity() = 0;
};

#endif /* JOURNAL_LEASE_H_ */
