/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:   config_parser.h
* Author: 
* Date:         2016/08/23
* Version:      1.0
* Description:
* 
************************************************/
#ifndef CONFIG_PARSER_H_
#define CONFIG_PARSER_H_
#include <boost/property_tree/ptree.hpp>
#include <string>
#include <vector>
using std::string;
class ConfigParser {
private:
    const char* file_;
    boost::property_tree::ptree pt_;
public:
    ConfigParser(const char* file);
    template<class Type>
        bool get_array(const char* key,std::vector<Type> &v);
    template<class Type>
        bool get(const char* path,Type& value) const;
    template<class Type>
        Type get_default(const char* path,const Type& default_value) const;
};
#include "config_parser_impl.h"
#define DEFAULT_CONFIG_FILE "/etc/storage-gateway/config.ini"
#endif
