/*
 * ceph_s3_lease.cc
 *
 *  Created on: 2016Äê8ÔÂ12ÈÕ
 *      Author: smile-luobin
 */

#include <cstring>
#include <thread>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "ceph_s3_lease.h"
#include "../include/libs3.h"

using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

RESULT CephS3LeaseClient::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int renew_window,
        int expire_window, int validity_window) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    renew_window_ = renew_window;
    expire_window_ = expire_window;
    validity_window_ = validity_window;
    stop_atomic_ = false;
    uuid_ = boost::lexical_cast<std::string>(
            uuid(boost::uuids::random_generator()()));

    if (acquire_lease()) {
        renew_thread_ptr_.reset(
                new std::thread(&CephS3Lease::renew_lease, this));
        return DRS_OK;
    } else {
        return INTERNAL_ERROR;
    }
}

std::string& CephS3LeaseClient::get_lease() {
    return uuid_;
}

bool CephS3LeaseClient::acquire_lease() {
    lease_expire_time_ = get_now_timestamp() + expire_window_;
    S3NameValue metadata("expire-time",
            std::to_string(lease_expire_time_).c_str());
    RESULT result = s3Api_ptr_->put_object(uuid_.c_str(), uuid_, metadata);
    if (result == DRS_OK) {
        return true;
    } else {
        return false;
    }
}

void CephS3LeaseClient::renew_lease() {
    while (!stop_atomic_.load()) {
        lease_expire_time_ = get_now_timestamp() + expire_window_;
        S3NameValue metadata("expire-time",
                std::to_string(lease_expire_time_).c_str());
        s3Api_ptr_->put_object(uuid_.c_str(), uuid_, metadata);

        std::this_thread::sleep_for(std::chrono::seconds(renew_window_));
    }
}

bool CephS3LeaseClient::check_lease_validity() {
    long now_time = get_now_timestamp();
    if (lease_expire_time_ - now_time > validity_window_) {
        return true;
    } else {
        return false;
    }
}

CephS3LeaseClient::~CephS3LeaseClient() {
    stop_atomic_.store(true);
    if(renew_thread_ptr_.get()){
        renew_thread_ptr_->join();
        renew_thread_ptr_.reset();
    }
}

RESULT CephS3LeaseServer::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int gc_interval) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    gc_interval_ = gc_interval;
    stop_atomic_ = false;
    gc_thread_ptr_.reset(new std::thread(&CephS3LeaseServer::gc_thread, this));
}

void CephS3LeaseServer::gc_thread() {
    while (!stop_atomic_.load()) {
        std::list<std::string> leases;
        RESULT result = s3Api_ptr_->list_objects(NULL, NULL, -1, leases);
        if (result == DRS_OK) {
            for (auto lease : leases) {
                S3NameValue metadata("expire-time", "");
                s3Api_ptr_->head_object(lease.c_str(), std::ref(metadata));
                long now_time = get_now_timestamp();
                //delete the expired lease
                if (std::strcmp(metadata.value,
                        std::to_string(now_time).c_str()) < 0) {
                    s3Api_ptr_->delete_object(lease);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(gc_interval));
    }
}

bool CephS3LeaseServer::check_lease_existance(const std::string& uuid) {
    std::string value;
    S3NameValue metadata("expire-time", "");
    RESULT result = s3Api_ptr_->head_object(uuid.c_str(), std::ref(metadata));

    if (result != DRS_OK) {
        return false;
    } else {
        long now_time = get_now_timestamp();
        if (std::strcmp(metadata.value,
                std::to_string(now_time).c_str()) > 0) {
            return true;
        } else {
            return false;
        }
    }
}

CephS3LeaseServer::~CephS3LeaseServer() {
    stop_atomic_.store(true);
    if (gc_thread_ptr_.get()) {
        gc_thread_ptr_->join();
        gc_thread_ptr_.reset();
    }
}
