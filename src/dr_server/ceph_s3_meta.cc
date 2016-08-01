/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    ceph_s3_meta.cc
* Author: 
* Date:         2016/07/13
* Version:      1.0
* Description:
* 
***********************************************/
#include <iostream>
#include <unistd.h>
#include <memory>
#include "ceph_s3_meta.h"
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif
#ifndef MAX_RETRIES
#define MAX_RETRIES 3
#endif
#define MAX_UPDATES 10
const string g_journal_id_key = "/journals/counters";
const string g_key_prefix = "/journals/";
const string g_marker_prefix = "/markers/";
const string g_replayer = "replayer";
const string g_replicator = "replicator";

// cephS3Meta member functions
CephS3Meta::CephS3Meta() {    
}
CephS3Meta::~CephS3Meta() {
}
RESULT CephS3Meta::init(const char* access_key, const char* secret_key, const char* host,
        const char* bucket_name) {
    RESULT res = DRS_OK;
    return res;
}

RESULT CephS3Meta::get_volume_journals(string vol_id, int limit,
                std::list<string>& list){
    return DRS_OK;
}
RESULT CephS3Meta::delete_journals(string vol_id, string journals[], int count){
    return DRS_OK;
}
RESULT CephS3Meta::seal_volume_journals(string vol_id, string journals[], int count) {
    return DRS_OK;
}

RESULT CephS3Meta::get_journal_marker(string vol_id, CONSUMER_TYPE type,
        JournalMarker* marker){
    return DRS_OK;
}
RESULT CephS3Meta::update_journals_marker(string vol_id, CONSUMER_TYPE type,
        JournalMarker marker){
    return DRS_OK;
}

RESULT CephS3Meta::get_consumer_journals(string vol_id,JournalMarker marker,
        int limit, std::list<string> &list) {
    return DRS_OK;
}

