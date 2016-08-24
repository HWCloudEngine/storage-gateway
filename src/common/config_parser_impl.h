/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:   config_parser_impl.h
* Author: 
* Date:         2016/08/23
* Version:      1.0
* Description:
* 
************************************************/
#ifndef CONFIG_PARSER_IMPL_H_
#define CONFIG_PARSER_IMPL_H_
#include <iostream>
#include <memory>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/exceptions.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "config_parser.h"

ConfigParser::ConfigParser(const char* file) {
    file_ = file;
    try {
        boost::property_tree::ini_parser::read_ini(file, pt_);
    }
    catch(boost::property_tree::ini_parser::ini_parser_error &e){
        std::cerr << e.what() << std::endl;
    }
}
template<class Type>
bool ConfigParser::get_array(const char* key,std::vector<Type> &v){
    try{
        string raw = pt_.get<std::string>(key);
        std::vector<string> _v;        
        boost::split(_v,raw,boost::is_any_of(",")); // split by ','        
        for(auto it=_v.begin();it!=_v.end();++it){ // convert string to Type
            boost::trim(*it); // trim
            v.push_back(boost::lexical_cast<Type>(*it));
        }
    }
    catch(boost::property_tree::ptree_error &e){
        std::cerr << e.what() << std::endl;
        return false;
    }
    catch(boost::bad_lexical_cast &e){
        std::cerr << e.what() << std::endl;
        return false;
    }
    return true;
}
template<class Type>
bool ConfigParser::get(const char* path,Type& value) const{
    try{
        value = pt_.get<Type>(path);
    }
    catch(boost::property_tree::ptree_error &e){
        std::cerr << e.what() << std::endl;
        return false;
    }
    return true;
}

template<class Type>
Type ConfigParser::get_default(const char* path,const Type& default_value) const{
    return pt_.get<Type>(path,default_value);    
}
#endif
