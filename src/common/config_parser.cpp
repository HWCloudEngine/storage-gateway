/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:   config_parser.cpp
* Author: 
* Date:         2016/08/23
* Version:      1.0
* Description:
* 
************************************************/
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
bool ConfigParser::getArray(const char* key,std::vector<Type> &v){
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

#if 0
/* test ini file:
[ceph_s3]
secret_key = abcccdslk234
port = 2381,  2382,  2384  
ips = 192.168.1.5, 192.168.1.6
is_true = false
d_test = 3.1253
access_key= 
*/

int main(int argc,char *argv[]){
    std::unique_ptr<ConfigParser> parser(new ConfigParser("config.ini"));
    string a;
    bool res = parser->get<string>("ceph_s3.secret_key",a);
    if(res == true)
        std::cout << "ceph_s3.secret_key=" << a << std::endl;
    else
        std::cout << "get ceph_s3.secret_key failed!" << std::endl;
    int nu;
    res = parser->get<int>("ceph_s3.not_exist",nu);
    if(res == false)
        std::cerr << "get ceph_s3.not_exist failed." << std::endl;
    res = parser->get<string>("ceph_s3.access_key",a);
    if(res == false)
        std::cerr << "get ceph_s3.access_key failed." << std::endl;
    else
        std::cout << "ceph_s3.access_key=" << a << std::endl;
    std::vector<int> v;
    res = parser->getArray("ceph_s3.port",v);
    if(res == true)
        for(auto it=v.begin();it!=v.end();it++)
            std::cout << "port: " << *it << std::endl;
    std::vector<string> vs;
    res = parser->getArray("ceph_s3.ips",vs);
    if(res == true)
        for(auto it=vs.begin();it!=vs.end();it++)
            std::cout << "ip: " << *it << std::endl;
    else
        std::cerr << "get ips failed." << std::endl;
    return 0;
}
#endif
