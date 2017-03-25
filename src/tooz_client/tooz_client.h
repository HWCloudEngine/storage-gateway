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
#include <list>
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
    void refresh_nodes_view(list<string> &nodes, string group_id);
    void rehash_buckets_to_node(string group_id, int bucket_num);
    string get_node(string bucket_id);
    list<string> get_buckets();
    string get_node_id();
private:
    PyObject *pInstance = NULL;
    thread *hb_t = NULL;
    thread *watch_t = NULL;
    thread *callback_t = NULL;
    list<string> buckets;
    string node_id;
    void get_env();
    void start_callback_server_thread(string group_id);
    void start_heartbeat_thread();
    void start_run_watchers_thread();
    void stop_callback_thread();
    void stop_run_watchers_thread();
    void stop_heartbeat_thread();
    void heartbeat();
    void callback_server(string group_id);
    int callback(int connection_fd, string group_id);
    void run_watchers();
};
