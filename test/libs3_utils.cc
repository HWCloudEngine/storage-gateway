/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    libs3_utils.cc
* Author: 
* Date:         2017/05/10
* Version:      1.0
* Description:
* 
************************************************/
#include "common/config_option.h"
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
        "   delete               : Delete a bucket or key\n"
        "     <bucket>[/<key>]   : Bucket or bucket/key to delete\n"
        "\n"
        "   list                 : List bucket contents\n"
        "\n"
        "   get                  : Gets an object\n"
        "     <buckey>/<key>     : Bucket/key of object to get\n"
        );
}

int get_f(CephS3Api& s3_meta, const char* key){
    string value;
    StatusCode res = s3_meta.get_object(key,&value);
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

int delete_key(CephS3Api& s3_meta, const char* key){
    if(StatusCode::sOk == s3_meta.delete_object(key)){
        std::cout << key << " is deleted.\n";
    }
    else{
        std::cout << "delete " << key << " failed!" << std::endl;
        return -1;
    }
    return 0;
}

int delete_all_keys(CephS3Api& s3_meta){
    std::list<std::string> list;
    StatusCode res = s3_meta.list_objects(NULL,NULL,0,&list,NULL);
    if(StatusCode::sOk == res){
        std::cout << "delete objects:" << ":\n";
        std::for_each(list.begin(),list.end(),[&](std::string& key){
            delete_key(s3_meta,key.c_str());
        });
    }
    else{
        std::cout << "list failed!" << std::endl;
        return -1;
    }
    return 0;
}

int list_f(CephS3Api& s3_meta,const std::string& bucket){
    std::list<std::string> list;
    StatusCode res = s3_meta.list_objects(NULL,NULL,0,&list,NULL);
    if(StatusCode::sOk == res){
        std::cout << "objects in " << bucket << ":\n";
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

    string access_key = g_option.ceph_s3_access_key;
    string secret_key = g_option.ceph_s3_secret_key;
    string host = g_option.ceph_host;
    string bucket_name = g_option.ceph_s3_bucket;
    CephS3Api s3_meta(access_key.c_str(),secret_key.c_str(),
        host.c_str(),bucket_name.c_str());

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
            result = get_f(s3_meta,argv[2]);
        }
        else if(strcmp(cmd,"list") == 0){
            result = list_f(s3_meta,bucket_name);
        }
        else if(strcmp(cmd,"delete") == 0){
            if(argc < 3 || argv[2] == NULL){
                result = delete_all_keys(s3_meta);
            }
            else{
                result = delete_key(s3_meta,argv[2]);
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
