/*************************************************************************
    > File Name: sg-tooz.c
    > Author: h00277488
    > Mail: huangguoqiang2@huawei.com
    > Created Time: Wed 15 Mar 2017 04:42:23 PM CST
 ************************************************************************/
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include "tooz_client.h"
atomic_long terminate_heartbeat(1);
atomic_long terminate_run_watchers(1);
atomic_long terminate_callback(1);
mutex py_instance_mutex;
//TODO use log instead of printf
ToozClient::ToozClient(){
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL;
    Py_Initialize();
    get_env();
    pModule = PyImport_ImportModule(TOOZ_API);
    if(!pModule){
        PyErr_Print();
        printf("load tooz api file error\n");
    }
    pDict = PyModule_GetDict(pModule);
    if(!pDict){
        PyErr_Print();
        printf("load tooz module error\n");
    }
    pClass = PyDict_GetItemString(pDict, TOOZ_COORDINATION);
    if(!pClass){
        PyErr_Print();
        printf("load tooz class error\n");
    }
    pInstance = PyInstance_New(pClass, NULL, NULL);
    if(!pInstance){
        PyErr_Print();
        printf("load tooz instance error\n");
    }
}

ToozClient::~ToozClient(){
    Py_Finalize();
}
void ToozClient::get_env()
{
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('./')");
    return;
}

void ToozClient::heartbeat(){
    while(terminate_heartbeat){
        printf("c:heartbeat\n");
        py_instance_mutex.lock();
        PyObject_CallMethod(pInstance, TOOZ_HEARTBEAT, NULL);
        py_instance_mutex.unlock();
        sleep(HEARTBEAT_TIME);
    }
}

void ToozClient::run_watchers(){
    while(terminate_run_watchers){
        printf("c:run watcher\n");
        py_instance_mutex.lock();
        PyObject_CallMethod(pInstance, TOOZ_RUN_WATHCHERS, NULL);
        py_instance_mutex.unlock();
        sleep(HEARTBEAT_TIME);
    }
}

void ToozClient::start_coordination(string backend_url, string member_id){
    node_id = member_id;
    if(backend_url.empty()){
        printf("backed_url is empty\n");
        return;
    }
    py_instance_mutex.lock();
    if(!member_id.empty()){
        PyObject_CallMethod(pInstance, TOOZ_START, "(s,s)", backend_url.c_str(), node_id.c_str());
    }else{
        //member_id set to uuid if none
        PyObject_CallMethod(pInstance, TOOZ_START, "(s)", backend_url.c_str());
    }
    py_instance_mutex.unlock();
}

void ToozClient::join_group(string group_id){
    py_instance_mutex.lock();
    PyObject_CallMethod(pInstance, TOOZ_JOIN_GROUP, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    start_heartbeat_thread();
}

void ToozClient::leave_group(string group_id){
    py_instance_mutex.lock();
    PyObject_CallMethod(pInstance, TOOZ_LEAVE_GROUP, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
}

void ToozClient::watch_group(string group_id){
    py_instance_mutex.lock();
    PyObject_CallMethod(pInstance, TOOZ_WATCH_GROUP, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    start_callback_server_thread(group_id);
    start_run_watchers_thread();
}

//used by sgclient
void ToozClient::refresh_nodes_view(list<string> &nodes, string group_id){
    py_instance_mutex.lock();
    PyObject_CallMethod(pInstance, TOOZ_REFRESH_NODES_VIEW, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
}

void ToozClient::rehash_buckets_to_node(string group_id, int bucket_num=DEFAULT_BUCKET_NUM){
    py_instance_mutex.lock();
    PyObject *p_bucket_list = PyObject_CallMethod(pInstance, TOOZ_REHASH_BUCKETS_TO_NODES,
                                                "(s,i)", group_id.c_str(), bucket_num);
    py_instance_mutex.unlock();
    if(p_bucket_list){
        int size = PyList_GET_SIZE(p_bucket_list);
        int i;
        for(i=0; i<size; i++){
            PyObject *p_bucket = PyList_GetItem(p_bucket_list, i);
            string bucket = PyString_AsString(p_bucket);
            buckets.push_back(bucket);
        }
    }
}

list<string> ToozClient::get_buckets(){
    return buckets;
}

string ToozClient::get_node_id(){
    return node_id;
}

//use by sgclient
string ToozClient::get_node(string bucket_id){
    py_instance_mutex.lock();
    PyObject *node_list = PyObject_CallMethod(pInstance, TOOZ_GET_NODES, "(s)", bucket_id.c_str());
    py_instance_mutex.unlock();
    if(node_list && PyList_GET_SIZE(node_list) > 0){
        PyObject *p_node = PyList_GetItem(node_list, 0);
        string node = PyString_AsString(p_node);
        return node;
    }else{
        return "";
    }
}

void ToozClient::stop_coordination(){
    stop_heartbeat_thread();
    stop_run_watchers_thread();
    stop_callback_thread();
    py_instance_mutex.lock();
    PyObject_CallMethod(pInstance, TOOZ_STOP, NULL);
    py_instance_mutex.unlock();
}

void ToozClient::start_heartbeat_thread(){
    hb_t = new thread(&ToozClient::heartbeat, this);
}

void ToozClient::start_run_watchers_thread(){
    watch_t = new thread(&ToozClient::run_watchers, this);
}

void ToozClient::start_callback_server_thread(string group_id){
    callback_t = new thread(&ToozClient::callback_server, this, group_id);
}

void ToozClient::stop_heartbeat_thread(){
    terminate_heartbeat= 0;
    if(hb_t){
        hb_t->join();
    }
}

void ToozClient::stop_run_watchers_thread(){
    terminate_run_watchers= 0;
    if(watch_t){
        watch_t->join();
    }
}

//TODO need to catch signal and close fd safely
void ToozClient::stop_callback_thread(){
    terminate_callback = 0;
    if(callback_t){
        //callback_t->join();
    }
}

int ToozClient::callback(int connection_fd, string group_id)
{
//TODO: use message queue
    int nbytes;
    char buffer[BUF_MAX_LEN];
    nbytes = read(connection_fd, buffer, BUF_MAX_LEN);
    buffer[nbytes] = 0;
    printf("get one message from tooz callback\n");
    printf("message: %s\n", buffer);
    rehash_buckets_to_node(group_id);
    return 0;
}

void ToozClient::callback_server(string group_id){
    struct sockaddr_un address;
    int socket_fd, connection_fd;
    socklen_t address_length;
    pid_t child;
    printf("start callback server\n");
    socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
    if(socket_fd < 0)
    {
        printf("socket() failed\n");
        return ;
    }
    unlink(SOCKET_FILE);
    memset(&address, 0, sizeof(struct sockaddr_un));
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, PATH_MAX, "%s",SOCKET_FILE);
    if(bind(socket_fd,
         (struct sockaddr *) &address,
         sizeof(struct sockaddr_un)) != 0)
    {
        printf("bind() failed\n");
        return ;
    }
    if(listen(socket_fd, 5) != 0)
    {
        printf("listen() failed\n");
        return ;
    }
    if((connection_fd = accept(socket_fd,
                               (struct sockaddr *) &address,
                               &address_length)) > -1)
    {
        while(terminate_callback){
            callback(connection_fd, group_id);
        }
    }
    close(connection_fd);
    close(socket_fd);
    unlink(SOCKET_FILE);
    return ;
}

