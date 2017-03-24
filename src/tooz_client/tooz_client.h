/*************************************************************************
    > File Name: sg-tooz.h
    > Author: h00277488
    > Mail: huangguoqiang2@huawei.com
    > Created Time: Wed 15 Mar 2017 04:42:23 PM CST
 ************************************************************************/
#include <thread>
#include <string>
#include <mutex>
#include <Python.h>
#include <atomic>
#include "tooz_api.h"

#define BUF_MAX_LEN     4096
using namespace std;
class ToozClient
{
public:
    ToozClient();
    ~ToozClient();
    void start_coordination(string backend_url, string member_id);
    void join_group(string group_id);
    void leave_group(string group_id);
    void watch_group(string group_id);
    void stop_coordination();
private:
    PyObject *pInstance = NULL;
    thread *hb_t;
    thread *watch_t;
    thread *callback_t;
    void get_env();
    void start_callback_server_thread();
    void start_heartbeat_thread();
    void start_run_watchers_thread();
    void stop_callback_thread();
    void stop_run_watchers_thread();
    void stop_heartbeat_thread();
    void heartbeat();
    void callback_server();
    int callback(int connection_fd);
    void run_watchers();
};
