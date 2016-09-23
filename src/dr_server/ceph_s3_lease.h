/*
 * ceph_s3_lease.h
 *
 *  Created on: 2016Äê8ÔÂ12ÈÕ
 *      Author: smile-luobin
 */

#ifndef CEPH_S3_LEASE_H_
#define CEPH_S3_LEASE_H_

#include <string>
#include <boost/thread/thread.hpp>
#include "ceph_s3_api.h"
#include "journal_lease.h"

class CephS3LeaseClient: public LeaseClient {
private:
    std::unique_ptr<CephS3Api> s3Api_ptr_;
    std::unique_ptr<boost::thread> renew_thread_ptr_;
    std::string uuid_;
    std::string prefix_;

    long lease_expire_time_;
    int expire_window_;
    int renew_window_;
    int validity_window_;

    virtual bool acquire_lease();
    virtual void renew_lease();
public:
    RESULT init(const char* access_key, const char* secret_key,
            const char* host, const char* bucket_name, int renew_window,
            int expire_window, int validity_window);
    std::string& get_lease();
    virtual bool check_lease_validity();
    virtual ~CephS3LeaseClient();
};

class CephS3LeaseServer: public LeaseServer {
private:
    int gc_interval_;
    std::string prefix_;
    std::unique_ptr<CephS3Api> s3Api_ptr_;
    std::unique_ptr<boost::thread> gc_thread_ptr_;
    void gc_task();
public:
    RESULT init(const char* access_key, const char* secret_key,
            const char* host, const char* bucket_name, int gc_interval);
    virtual bool check_lease_existance(const std::string&);
    virtual ~CephS3LeaseServer();
};
#endif /* CEPH_S3_LEASE_H_ */
