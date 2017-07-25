/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_meta_utils.cc
* Author: 
* Date:         2017/05/10
* Version:      1.0
* Description:
* 
************************************************/
#include "common/config_option.h"
#include "common/kv_api.h"
#include "common/db_meta_api.h"
#include "common/ceph_s3_api.h"
#include "rpc/volume.pb.h"
#include "rpc/journal/journal.pb.h"
#include "log/log.h"
#include <string>
#include <stdio.h>
#include <iostream>
#include <algorithm>
using huawei::proto::JournalMarker;
using huawei::proto::JournalMeta;
using huawei::proto::VolumeMeta;

void print_usage(FILE* out){
    fprintf(out,
        "\n Usage:"
        "\n Commands (with <required parameters> and [optional parameters]) :"
        "\n"
        "   delete               : Delete a specified key or (empty)all keys\n"
        "     [key]  : Bucket or bucket/key to delete\n"
        "\n"
        "   list                 : List all keys\n"
        "     [marker] : Marker to start at \n"
        "\n"
        "   get                  : Gets an object\n"
        "     <key>              : key of object to get\n"
        );
}

int get_f(KVApi& meta, const char* key){
    string value;
    StatusCode res = meta.get_object(key,&value);
    if(StatusCode::sOk != res){
        std::cout << "get object " << key << " failed!" << std::endl;
        return -1;
    }
    string keys(key);
    if(keys.find("/markers/") != std::string::npos){
        JournalMarker marker;
        if(marker.ParseFromString(value)){
            std::cout << key << ":\n"
                << "journal: " << marker.cur_journal() << "\n"
                << "offset: " << marker.pos() << "\n";
        }
        else{
            std::cout << "parse " << key << " failed!" << std::endl;
            return -1;
        }
    }
    else if(keys.find("/volumes/") != std::string::npos){
        VolumeMeta vol_meta;
        if(vol_meta.ParseFromString(value)){
            std::cout << key << ":\n"
                << "volume id: " << vol_meta.info().vol_id() << "\n"
                << "path:" << vol_meta.info().path() << "\n"
                << "volume status: " << vol_meta.info().vol_status() << "\n"
                << "volume size: " << vol_meta.info().size() << "\n"
                << "attached host:" << vol_meta.info().attached_host() << "\n"
                << "rep enable: " << vol_meta.info().rep_enable() << "\n";
            if(vol_meta.info().rep_enable()){
                std::cout << "rep uuid: " << vol_meta.info().rep_uuid() << "\n"
                    << "rep role: " << vol_meta.info().role() << "\n"
                    << "rep status: " << vol_meta.info().rep_status() << "\n"
                    << "peer volume: " << vol_meta.info().peer_volumes(0) << "\n";
            }
        }
        else{
            std::cout << "parse " << key << " failed!" << std::endl;
            return -1;
        }
    }
    else if(keys.find("/journals/") != std::string::npos){
        JournalMeta journal;
        if(journal.ParseFromString(value)){
            std::cout << key << ":\n"
                << "path: " << journal.path() << "\n"
                << "status: " << journal.status() << "\n";
        }
        else{
            std::cout << "parse " << key << " failed!" << std::endl;
            return -1;
        }
    }
    else{
        std::cout << key << ":\n" << value << std::endl;
    }
    return 0;
}

int delete_key(KVApi& meta, const char* key){
    if(StatusCode::sOk == meta.delete_object(key)){
        std::cout << key << " is deleted.\n";
    }
    else{
        std::cout << "delete " << key << " failed!" << std::endl;
        return -1;
    }
    return 0;
}

int delete_all_keys(KVApi& meta){
    std::list<std::string> list;
    StatusCode res = meta.list_objects(NULL,NULL,0,&list,NULL);
    if(StatusCode::sOk == res){
        std::cout << "delete objects:" << ":\n";
        std::for_each(list.begin(),list.end(),[&](std::string& key){
            delete_key(meta,key.c_str());
        });
    }
    else{
        std::cout << "list failed!" << std::endl;
        return -1;
    }
    return 0;
}

int list_f(KVApi& meta,const char* marker){
    std::list<std::string> list;
    StatusCode res = meta.list_objects("/",marker,0,&list,NULL);
    if(StatusCode::sOk == res){
        std::cout << "list objects:\n";
        std::for_each(list.begin(),list.end(),[](std::string& key){
            std::cout << key << std::endl;
        });
    }
    else{
        std::cout << "list failed!" << std::endl;
        return -1;
    }
    return 0;
}

int main(int argc,char* argv[]){
    DRLog::log_init("sg_tool.log");
    std::shared_ptr<KVApi> kvApi_ptr;
    if(g_option.global_storage_media.compare("ceph") == 0) {
        kvApi_ptr.reset(new CephS3Api(
                        g_option.ceph_s3_access_key.c_str(),
                        g_option.ceph_s3_secret_key.c_str(),
                        g_option.ceph_host.c_str(),
                        g_option.ceph_s3_bucket.c_str()));
    }
    else {
        string db_path = g_option.local_meta_path + "/journals/meta_db";
        if (access(db_path.c_str(), F_OK)) {
            char cmd[256] = "";
            snprintf(cmd, sizeof(cmd), "mkdir -p %s", db_path.c_str());
            int ret = system(cmd);
            SG_ASSERT(ret != -1);
        }
        IndexStore* index_store = IndexStore::create("rocksdb", db_path);
        kvApi_ptr.reset(new DBMetaApi(index_store));
    }

    int result = 1;
    do {
        if(argc < 2){
            break;
        }
        const char* cmd = argv[1];
        if(strcmp(cmd,"get") == 0){
            if(argc < 3 || argv[2] == NULL){
                std::cout << "invalid argument!" << std::endl;
                break;
            }
            result = get_f(*kvApi_ptr,argv[2]);
        }
        else if(strcmp(cmd,"list") == 0){
            if(argc < 3 || argv[2] == NULL){
                result = list_f(*kvApi_ptr,NULL);
            }
            else {
                result = list_f(*kvApi_ptr,argv[2]);
            }
        }
        else if(strcmp(cmd,"delete") == 0){
            if(argc < 3 || argv[2] == NULL){
                result = delete_all_keys(*kvApi_ptr);
            }
            else{
                result = delete_key(*kvApi_ptr,argv[2]);
            }
        }
        else{
            break;
        }
    }while(false);
    if(result > 0){
        print_usage(stdout);
    }
    return result;
}
