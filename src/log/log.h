/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    log.h
* Author: 
* Date:         2016/07/11
* Version:      1.0
* Description:
* 
**********************************************/
#ifndef LOG_H_
#define LOG_H_
#include <boost/log/sources/logger.hpp>
#include <boost/log/attributes/named_scope.hpp>
#include <boost/log/utility/manipulators/add_value.hpp>
#include <boost/log/common.hpp>
#include <string>
#include <assert.h>
#define LOG_PATH "/var/log/storage-gateway"
// define severity levels
typedef enum severity_level
{
    SG_TRACE,
    SG_DEBUG,
    SG_INFO,
    SG_WARN,
    SG_ERR,
    SG_FATAL
}severity_level_t;

extern void print_backtrace();
extern void signal_handler(int sig);
extern std::string path_to_filename(std::string path);

class DRLog{
public:
    static ::boost::log::sources::severity_logger_mt<severity_level> slg;
    static void log_init(std::string file_name); // set log file name
    static void set_log_level(severity_level_t level);
};

#define BOOST_SLOG(log_level) \
    BOOST_LOG_SEV( DRLog::slg, log_level) \
  << boost::log::add_value("Line", __LINE__) \
  << boost::log::add_value("File", path_to_filename(__FILE__))

#define LOG_FATAL BOOST_SLOG(SG_FATAL)
#define LOG_ERROR BOOST_SLOG(SG_ERR)
#define LOG_WARN BOOST_SLOG(SG_WARN)
#define LOG_INFO BOOST_SLOG(SG_INFO)
#define LOG_DEBUG BOOST_SLOG(SG_DEBUG)
#define LOG_TRACE BOOST_SLOG(SG_TRACE)
#define SG_ERROR_OCCURED() \
    do { \
        LOG_ERROR << "Internal error occured!"; \
        print_backtrace(); \
        assert(0); \
        exit(1); \
    }while(0);
#define SG_ASSERT(x) \
    if(!(x)){ \
        LOG_ERROR << "Assert failed:"#x"\n"; \
        SG_ERROR_OCCURED(); \
    }
#define SHOW_CALL_STACK BOOST_LOG_FUNCTION()
#define SET_ACOPE_NAME(name) BOOST_LOG_NAMED_SCOPE(name)
#endif
