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
#include <sstream>
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
        string raw = pt_.get<string>(key);
        std::vector<string> _v;
        int i=0,pos=0;
        // split by ','
        for(;i<raw.length();i++){
            if(',' == raw.at(i)){
                string str = raw.substr(pos,i-pos);
                // trim
                str.erase(0,str.find_first_not_of(' '));
                str.erase(str.find_last_not_of(' ')+1);
                _v.push_back(str);
                pos = i+1;
            }                
        }
        if(pos < raw.length()){
            string str = raw.substr(pos,raw.length() - pos);
             // trim
            str.erase(0,str.find_first_not_of(' '));
            str.erase(str.find_last_not_of(' ')+1);
            _v.push_back(str);
        }
        
        for(auto it=_v.begin();it!=_v.end();++it){ // convert string to Type
//            std::cout << "substr:" << *it << std::endl; //debug
            Type value;
            std::istringstream iss(*it);
            iss >> value;
            v.push_back(value);
        }
    }
    catch(boost::property_tree::ptree_error &e){
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
