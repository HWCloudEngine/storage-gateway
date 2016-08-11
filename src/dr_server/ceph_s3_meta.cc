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
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include "ceph_s3_meta.h"
#include "log/log.h"
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif
#ifndef MAX_RETRIES
#define MAX_RETRIES 3
#endif
#define MAX_JOURNAL_COUNTER 1000
#define MIN_JOURNAL_COUNTER 1
const string g_key_prefix = "/journals/";
const string g_marker_prefix = "/markers/";
const string g_opened = "/opened/";
const string g_sealed = "/sealed/";
const string g_replayer = "replayer";
const string g_replicator = "replicator";

void GC_task(){
    while(CephS3Meta::_thread_GC_running){
		// TODO: journal GC
        sleep(1);
    }
}
RESULT CephS3Meta::init_journal_name_counter(const string vol_id,int64_t& cnt){
    return DRS_OK;
}
RESULT CephS3Meta::get_journal_name_counter(const string vol_id,int64_t& cnt){
    return DRS_OK;
}
RESULT CephS3Meta::add_journal_name_counter(const string vol_id,const int64_t add){
    return DRS_OK;
}
// cephS3Meta member functions
CephS3Meta::CephS3Meta() {    
}
CephS3Meta::~CephS3Meta() {
}
RESULT CephS3Meta::init(const char* access_key, const char* secret_key, const char* host,
        const char* bucket_name, const char* path) {
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

// TODO:GC thread to check and delete unwanted journal files and their metas, and seal journals of crushed client
bool CephS3Meta::_thread_GC_running = false;
void CephS3Meta::init_GC_thread(){
    if(CephS3Meta::_thread_GC_running) {
        LOG_WARN << "journals GC thread was already init.";
        return;
    }
    CephS3Meta::_thread_GC_running = true;
    _thread_GC = ::std::thread(GC_task);    
    LOG_INFO << "init GC thread, id=" << _thread_GC.get_id();
    return;
}


