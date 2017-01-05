#ifndef JOURNAL_LEASE_H_
#define JOURNAL_LEASE_H_

#include <chrono>
#include <string>
#include "rpc/common.pb.h"

using huawei::proto::RESULT;

class LeaseClient {
private:
    virtual bool acquire_lease() = 0;
    virtual void renew_lease() = 0;
public:
    virtual ~LeaseClient() {};
    virtual bool check_lease_validity(const std::string&) = 0;
};

class LeaseServer {
public:
    virtual ~LeaseServer() {};
    virtual bool check_lease_existance(const std::string&) = 0;
};

#endif /* JOURNAL_LEASE_H_ */
