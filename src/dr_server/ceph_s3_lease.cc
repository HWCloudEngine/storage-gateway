/*
 * ceph_s3_lease.cc
 *
 *  Created on: 2016Äê8ÔÂ12ÈÕ
 *      Author: smile-luobin
 */

#include <chrono>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "ceph_s3_lease.h"
#include "../include/libs3.h"

using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

RESULT CephS3Lease::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int renew_window,
        int expire_window, int validity_window) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    renew_window_ = renew_window;
    expire_window_ = expire_window;
    validity_window_ = validity_window;
    stop_atomic_ = false;
    uuid_ = boost::lexical_cast<std::string>(uuid(boost::uuids::random_generator()()));

    if (acquire_lease()) {
        renew_thread_ptr_.reset(new std::thread(renew_lease));
        return DRS_OK;
    } else {
        return INTERNAL_ERROR;
    }

}

bool CephS3Lease::acquire_lease() {
    try {
        lease_expire_time_ = get_now_timestampe() + expire_window_;
        S3NameValue metadata("expire_time",
                std::to_string(lease_expire_time_).c_str());
        s3Api_ptr_->put_object(uuid_.c_str(), uuid_, metadata);
        return true;
    } catch (...) {
        return false;
    }
}

long CephS3Lease::get_now_timestampe() {
    using namespace std::chrono;
    time_point<system_clock, seconds> today = time_point_cast<seconds>(
            system_clock::now());
    return static_cast<long>(today.time_since_epoch().count());
}

void CephS3Lease::renew_lease() {
    while (!stop_atomic_.load()) {
        try {
            lease_expire_time_ = get_now_timestampe() + expire_window_;
            S3NameValue metadata("expire_time",
                            std::to_string(lease_expire_time_).c_str());
            s3Api_ptr_->put_object(uuid_.c_str(), uuid_, metadata);
        } catch (...) {
            std::this_thread::sleep_for(std::chrono::seconds(renew_window_));
        }
        std::this_thread::sleep_for(std::chrono::seconds(renew_window_));
    }
}

bool CephS3Lease::check_lease_validity() {
    long now_time = get_now_timestampe();
    if (lease_expire_time_ - now_time > validity_window_) {
        return true;
    } else {
        return false;
    }
}

CephS3Lease::~CephS3Lease(){
    stop_atomic_.store(true);
    renew_thread_ptr_->join();
}
