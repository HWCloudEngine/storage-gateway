/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    log.cc
* Author: 
* Date:         2016/07/11
* Version:      1.0
* Description:
* 
**********************************************/
#include <execinfo.h>
#include <iostream>
#include <iomanip>
#include <boost/smart_ptr.hpp>
#include <boost/log/expressions.hpp>
//#include <boost/log/utility/empty_deleter.hpp>
//#include <boost/log/utility/setup/console.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
//#include  <boost/log/sources/record_ostream.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <grpc/support/log.h>
#include "log.h"
#define BT_BUF_SIZE 128
namespace logging = boost::log;
namespace src = boost::log::sources;
namespace expr = boost::log::expressions;
namespace sinks = boost::log::sinks;
namespace attrs = boost::log::attributes;
namespace keywords = boost::log::keywords;

::boost::log::sources::severity_logger_mt<severity_level> DRLog::slg;

static const char* strings[] =
{
    "TRACE",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "FATAL"
};

// The operator puts a human-friendly representation of the severity level to the stream
std::ostream& operator<< (std::ostream& strm, severity_level level)
{    
    if (static_cast< std::size_t >(level) < sizeof(strings) / sizeof(*strings))
        strm << strings[level];
    else
        strm << static_cast< int >(level);

    return strm;
}

// gprc default log
void grpc_log_fun(gpr_log_func_args *args) {
    const char* display_file;
    const char* final_slash = strrchr(args->file, '/');
    if (final_slash == NULL)
        display_file = args->file;
    else
        display_file = final_slash + 1;

    severity_level level = (args->severity == GPR_LOG_SEVERITY_ERROR) ?
        SG_ERR : SG_DEBUG;

    BOOST_LOG_SEV( DRLog::slg, level)
        << boost::log::add_value("Line", args->line)
        << boost::log::add_value("File", display_file)
        << "@grpc:"
        << args->message;
}

BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", severity_level)
BOOST_LOG_ATTRIBUTE_KEYWORD(timestamp, "TimeStamp", boost::posix_time::ptime)
BOOST_LOG_ATTRIBUTE_KEYWORD(scope, "Scope", attrs::named_scope::value_type)

void DRLog::log_init(std::string file_name)
{
    // Setup the common formatter for all sinks
    logging::formatter fmt = expr::stream
        << expr::format_date_time(timestamp, "[%Y-%m-%d,%H:%M:%S.%f]")
        << std::setfill(' ') << " <" << std::left << std::setw(5) << severity << "> "
        << "[" << expr::attr<attrs::current_thread_id::value_type>("ThreadID") << "] "
        << '[' << expr::attr<std::string>("File")
        << ':' << expr::attr<int>("Line") << "] "
        << expr::if_(expr::has_attr(scope))
            [
                expr::stream << expr::format_named_scope(scope,
                   // keywords::depth = 3, // limits the call stack
                   // keywords::iteration = expr::reverse,
                    keywords::format = "%n "
                    )
            ]
        << ": " << expr::smessage;
    // Initialize backend sink
    boost::shared_ptr<logging::core> core = logging::core::get();
    boost::shared_ptr<sinks::text_file_backend> backend =
        boost::make_shared<sinks::text_file_backend>(
            keywords::file_name =  file_name + ".%Y-%m-%d_%H-%M-%S", // file name
            keywords::rotation_size = 5 * 1024 * 1024, // log file size
            keywords::open_mode = std::ios_base::app | std::ios_base::out  // append if same log file
            );

    // TODO: The frontend can be synchronous_sink or asynchronous_sink
    // the synchronous_sink is helpful fo debugging, but it's performance is a bit poor
    typedef sinks::synchronous_sink<sinks::text_file_backend> sink_t;
    boost::shared_ptr<sink_t> file_sink(new sink_t(backend)); // Wrap it into the frontend
    file_sink->set_formatter(fmt); // set format
    file_sink->locked_backend()->set_file_collector(sinks::file::make_collector(
        keywords::target=LOG_PATH,        // log files location
        keywords::max_size=500*1024*1024,    // max size of log files
        keywords::min_free_space=100*1024*1024  // min free size of the disk
    ));
    // TODO: disable auto_flush for high performance
    file_sink->locked_backend()->auto_flush(true);
    // Upon restart, scan the directory for files matching the file_name pattern
    file_sink->locked_backend()->scan_for_files();
    // register in the core.
    core->add_sink(file_sink);
    // Add attributes
    logging::add_common_attributes();
    logging::core::get()->add_global_attribute("Scope", attrs::named_scope());
    // config grpc log
    gpr_set_log_function(grpc_log_fun);
}

void DRLog::set_log_level(severity_level_t level)
{
    if (static_cast<std::size_t>(level) < sizeof(strings) / sizeof(*strings))
        logging::core::get()->set_filter(severity >= level);
}

void print_backtrace(){
    int i,n;
    void *buffer[BT_BUF_SIZE];
    char **strings;
    
    n = backtrace(buffer, BT_BUF_SIZE);
    /* The call backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO)
    would produce similar output to the following: */

    strings = backtrace_symbols(buffer, n);
    if (strings == NULL) {
        std::cerr << "backtrace_symbols failed!" << std::endl;
        LOG_FATAL << "backtrace_symbols failed!";
        exit(1);
    }

    for (i = 0; i < n; i++){
        std::cerr << strings[i] << std::endl;
        LOG_FATAL << strings[i];
    }

    free(strings);
}

void signal_handler(int sig) {
    LOG_FATAL << "CAUSE of ERROR signal:" << sig;
    print_backtrace();
    exit(1);
}

// Convert file path to only the filename
std::string path_to_filename(std::string path) {
   return path.substr(path.find_last_of("/\\")+1);
}

#if 0
void zxxx(void)
{
    SHOW_CALL_STACK; // print scope attr value
    LOG_INFO << "test in zxxx.";
}
int main(int argc, char** argv)
{
    std::string file="drserver.log";
    DRLog::log_init(file);
    LOG_TRACE << "A trace severity message";
    LOG_DEBUG << "A debug severity message";
    LOG_INFO << "An informational severity message";
    LOG_WARN << "A warning severity message";
    LOG_ERROR << "An error severity message";
    LOG_FATAL << "A fatal severity message";
    zxxx();
    return 0;
}
#endif
