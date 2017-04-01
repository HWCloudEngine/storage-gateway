/*************************************************************************
    > File Name: sg-tooz.h
    > Author: h00277488
    > Mail: huangguoqiang2@huawei.com
    > Created Time: Wed 15 Mar 2017 04:42:23 PM CST
 ************************************************************************/
#ifndef _TOOZ_CLIENT_H
#define _TOOZ_CLIENT_H

#include <thread>
#include <string>
#include <mutex>
#include <Python.h>
#include <atomic>
#include <list>
#include "tooz_api.h"
#include "callback_interface.h"
#define BUF_MAX_LEN     4096
using namespace std;
class ToozClient
{
public:
    ToozClient();
    ~ToozClient();
    //connect to tooz, backend_url="driver_type://ip:port", member_id will be set to uuid if None
    void start_coordination(string backend_url, string member_id);

    //join group and send heartbeat to tooz periodically
    void join_group(string group_id);
    void leave_group(string group_id);

    //watch group changed.
    void watch_group(string group_id);

    //disconnet with tooz
    void stop_coordination();

    //use consistent hashring to rebalance buckets to node
    void rehash_buckets_to_node(string group_id, int bucket_num);

    //used only by sg-client. Get new node view from tooz. It's a short connection. connection will open/close automatically
    void refresh_nodes_view(string backend_url, string group_id);

    //used only sg-client, calculate node locally by hashring
    string get_node(string bucket_id);

    //get bucket list belong to sg-server
    list<string> get_buckets();

    //get sg-server node-id, such as "ip:port"
    string get_node_id();

    //register callback function, trigger when group changed
    void register_callback(ICallback *callback);
    void unregister_callback(ICallback *callback);

private:
    //used by embedding python, python parser instance
    PyObject *pInstance = NULL;
    thread *hb_t = NULL;
    thread *watch_t = NULL;
    thread *callback_t = NULL;

    /*each node takes control some buckets,
    *old_buckets means bucket list before group changed
    *new_buckets means bucket list after group changed
    */
    list<string> old_buckets;
    list<string> new_buckets;

    //unique id of each node, such as "ip:port"
    string node_id;

    //callback function list registered
    list<ICallback *> callback_list;

    //get environment used by "tooz_api.py"
    void get_env();

    //start 3 threads(callback_server/heartbeat/run_watchers), deal with group-changed scenarios.
    void start_callback_server_thread(string group_id);
    void start_heartbeat_thread();
    void start_run_watchers_thread();
    void stop_callback_thread();
    void stop_run_watchers_thread();
    void stop_heartbeat_thread();
    void heartbeat();

    //start a uds server, listen notification sent by "tooz_api.py"
    void callback_server(string group_id);

    //run registered callback function
    int callback(char *event_type);

    //check whether group changed when watch_group
    void run_watchers();
};

#endif
