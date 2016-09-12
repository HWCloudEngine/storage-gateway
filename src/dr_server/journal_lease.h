/*
 * journal_lease.h
 *
 *  Created on: 2016��8��12��
 *      Author: smile-luobin
 */

#ifndef JOURNAL_LEASE_H_
#define JOURNAL_LEASE_H_

#include <chrono>
#include <string>
#include "rpc/common.pb.h"

using huawei::proto::RESULT;

class LeaseClient {
public:
    virtual ~LeaseClient();
    virtual bool acquire_lease() = 0;
    virtual void renew_lease() = 0;
    virtual bool check_lease_validity() = 0;
};

class LeaseServer {
public:
    virtual ~LeaseServer();
    virtual bool check_lease_existance(const std::string&) = 0;
};

#endif /* JOURNAL_LEASE_H_ */
