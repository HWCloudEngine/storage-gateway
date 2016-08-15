/*
 * lease_manager.h
 *
 *  Created on: 2016Äê8ÔÂ12ÈÕ
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
    virtual bool check_lease_existance(const std::string&) = 0;
};

long get_now_timestamp() {
    using namespace std::chrono;
    time_point<system_clock, seconds> today = time_point_cast<seconds>(
            system_clock::now());
    return static_cast<long>(today.time_since_epoch().count());
}

#endif /* JOURNAL_LEASE_H_ */
