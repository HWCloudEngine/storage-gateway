/*
 * ceph_s3_lease.cc
 *
 *  Created on: 2016Äê8ÔÂ12ÈÕ
 *      Author: smile-luobin
 */

#include <map>
#include <chrono>
#include <time.h>
#include <thread>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "ceph_s3_lease.h"

using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

RESULT CephS3LeaseClient::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int renew_window,
        int expire_window, int validity_window) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    renew_window_ = renew_window;
    expire_window_ = expire_window;
    validity_window_ = validity_window;
    prefix_ = "/leases/";
    uuid_ = boost::uuids::to_string(boost::uuids::uuid(boost::uuids::random_generator()()));

    if (acquire_lease()) {
        renew_thread_ptr_.reset(
                new boost::thread(
                        boost::bind(&CephS3LeaseClient::renew_lease, this)));
        return DRS_OK;
    } else {
        return INTERNAL_ERROR;
    }
}

std::string& CephS3LeaseClient::get_lease() {
    return uuid_;
}

bool CephS3LeaseClient::acquire_lease() {
    lease_expire_time_ = static_cast<long>(time(NULL)) + expire_window_;
    std::map<std::string, std::string> metadata;
    metadata["expire-time"] = std::to_string(lease_expire_time_);
    std::string lease_key = prefix_ + uuid_;
    RESULT result = s3Api_ptr_->put_object(lease_key.c_str(), &uuid_,
            &metadata);
    if (result == DRS_OK) {
        return true;
    } else {
        return false;
    }
}

void CephS3LeaseClient::renew_lease() {
    while (true) {
        lease_expire_time_ = static_cast<long>(time(NULL)) + expire_window_;
        std::map<std::string, std::string> metadata;
        metadata["expire-time"] = std::to_string(lease_expire_time_);
        std::string lease_key = prefix_ + uuid_;
        RESULT result = s3Api_ptr_->put_object(lease_key.c_str(), &uuid_,
                &metadata);

        std::this_thread::sleep_for(std::chrono::seconds(renew_window_));
    }
}

bool CephS3LeaseClient::check_lease_validity() {
    long now_time = static_cast<long>(time(NULL));
    if (lease_expire_time_ - now_time > validity_window_) {
        return true;
    } else {
        return false;
    }
}

CephS3LeaseClient::~CephS3LeaseClient() {
    if (renew_thread_ptr_.get()) {
        renew_thread_ptr_->interrupt();
        renew_thread_ptr_->join();
        renew_thread_ptr_.reset();
    }
}

RESULT CephS3LeaseServer::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int gc_interval) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    gc_interval_ = gc_interval;
    prefix_ = "/leases/";
    gc_thread_ptr_.reset(
            new boost::thread(boost::bind(&CephS3LeaseServer::gc_task, this)));
}

void CephS3LeaseServer::gc_task() {
    while (true) {
        std::list<std::string> leases;
        RESULT result = s3Api_ptr_->list_objects(NULL, NULL, NULL, -1, &leases);
        if (result == DRS_OK) {
            for (auto lease : leases) {
                std::map<std::string, std::string> metadata;
                s3Api_ptr_->head_object(lease.c_str(), &metadata);
                long now_time = static_cast<long>(time(NULL));
                //delete the expired lease
                if (metadata.find("expire-time") != metadata.end()) {
                    try {
                        long expire_time = std::stol(metadata["expire-time"]);
                        if (expire_time < now_time) {
                            s3Api_ptr_->delete_object(lease.c_str());
                        }
                    } catch (...) {
                    }
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(gc_interval_));
    }
}

bool CephS3LeaseServer::check_lease_existance(const std::string& uuid) {
    std::string value;
    std::map<std::string, std::string> metadata;
    std::string object_key = prefix_ + uuid;
    RESULT result = s3Api_ptr_->head_object(object_key.c_str(), &metadata);

    if (result != DRS_OK) {
        return false;
    } else {
        if (metadata.find("expire-time") != metadata.end()) {
            long now_time = static_cast<long>(time(NULL));
            try {
                long expire_time = std::stol(metadata["expire-time"]);
                if (expire_time < now_time) {
                    return false;
                } else {
                    return true;
                }
            } catch (...) {
                return true;
            }
        } else {
            return true;
        }
    }
}

CephS3LeaseServer::~CephS3LeaseServer() {
    if (gc_thread_ptr_.get()) {
        gc_thread_ptr_->interrupt();
        gc_thread_ptr_->join();
        gc_thread_ptr_.reset();
    }
}
