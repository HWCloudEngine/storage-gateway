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
#include <boost/log/common.hpp>
#include <string>
#include <assert.h>
#define LOG_PATH "/var/log/dr_service"
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
extern std::string path_to_filename(std::string path);
extern std::string set_get_attrib(const char* name, std::string value);
extern int set_get_attrib(const char* name, int value);
class DRLog{
public:
    static ::boost::log::sources::severity_logger<severity_level> slg;
    static void log_init(std::string file_name); // set log file name
    static void set_log_level(severity_level_t level);
};

#define BOOST_SLOG(log_level) \
    BOOST_LOG_STREAM_WITH_PARAMS( \
        (DRLog::slg), \
        (set_get_attrib("File", path_to_filename(__FILE__))) \
        (set_get_attrib("Line", __LINE__)) \
        (::boost::log::keywords::severity = log_level) \
    )

#define LOG_FATAL BOOST_SLOG(SG_FATAL)
#define LOG_ERROR BOOST_SLOG(SG_ERR)
#define LOG_WARN BOOST_SLOG(SG_WARN)
#define LOG_INFO BOOST_SLOG(SG_INFO)
#define LOG_DEBUG BOOST_SLOG(SG_DEBUG)
#define LOG_TRACE BOOST_SLOG(SG_TRACE)
#define DR_ERROR_OCCURED() \
    do { \
        LOG_ERROR << "Internal error occured!"; \
        print_backtrace(); \
        assert(0); \
        exit(1); \
    }while(0);
#define DR_ASSERT(x) \
    if(!(x)){ \
        LOG_ERROR << "Assert failed:"#x"\n"; \
        DR_ERROR_OCCURED(); \
    }
#define SHOW_CALL_STACK BOOST_LOG_FUNCTION()
#define SET_ACOPE_NAME(name) BOOST_LOG_NAMED_SCOPE(name)
#endif
