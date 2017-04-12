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
#include "tooz_client.h"
#include "log.h"
atomic_long terminate_heartbeat(1);
atomic_long terminate_run_watchers(1);
atomic_long terminate_callback(1);
mutex py_instance_mutex;

#include <iostream>
using namespace std;

//TODO use log instead of printf
ToozClient::ToozClient(){
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL;
    Py_Initialize();
    get_env();
    pModule = PyImport_ImportModule(TOOZ_API);
    if(!pModule){
        PyErr_Print();
        LOG_ERROR << "load tooz api file error";
    }
    pDict = PyModule_GetDict(pModule);
    if(!pDict){
        PyErr_Print();
        LOG_ERROR << "load tooz module error";
    }
    pClass = PyDict_GetItemString(pDict, TOOZ_COORDINATION);
    if(!pClass){
        PyErr_Print();
        LOG_ERROR << "load tooz class error";
    }
    pInstance = PyInstance_New(pClass, NULL, NULL);
    if(!pInstance){
        PyErr_Print();
        LOG_ERROR << "load tooz instance error";
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
        py_instance_mutex.lock();
        PyObject_CallMethod(pInstance, TOOZ_HEARTBEAT, NULL);
        py_instance_mutex.unlock();
        sleep(HEARTBEAT_TIME);
    }
}

void ToozClient::run_watchers(){
    while(terminate_run_watchers){
        py_instance_mutex.lock();
        PyObject_CallMethod(pInstance, TOOZ_RUN_WATHCHERS, NULL);
        py_instance_mutex.unlock();
        sleep(HEARTBEAT_TIME);
    }
}

int ToozClient::start_coordination(string backend_url, string member_id){
    node_id = member_id;
    if(backend_url.empty()){
        LOG_WARN << "backed_url is empty";
        return TOOZ_OK;
    }
    py_instance_mutex.lock();
    PyObject * result = NULL;
    if(!member_id.empty()){
        result = PyObject_CallMethod(pInstance, TOOZ_START, "(s,s)", backend_url.c_str(), node_id.c_str());
    }else{
        //member_id set to uuid if none
        result = PyObject_CallMethod(pInstance, TOOZ_START, "(s)", backend_url.c_str());
    }
    py_instance_mutex.unlock();
    int status = TOOZ_ERROR;
    if(result == NULL){
        PyErr_Print();
    }else{
        PyArg_Parse(result, "i", &status);
    }
    return status;
}

int ToozClient::join_group(string group_id){
    py_instance_mutex.lock();
    PyObject * result = NULL;
    result = PyObject_CallMethod(pInstance, TOOZ_JOIN_GROUP, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    int status = TOOZ_ERROR;
    if(result == NULL){
        PyErr_Print();
    }else{
        PyArg_Parse(result, "i", &status);
    }
    if(status == TOOZ_OK){
        LOG_DEBUG << "start heartbeat thread";
        start_heartbeat_thread();
    }
    return status;
}

int ToozClient::leave_group(string group_id){
    py_instance_mutex.lock();
    PyObject * result = NULL;
    result = PyObject_CallMethod(pInstance, TOOZ_LEAVE_GROUP, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    int status = TOOZ_ERROR;
    if(result == NULL){
        PyErr_Print();
    }else{
        PyArg_Parse(result, "i", &status);
    }
    return status;
}

int ToozClient::watch_group(string group_id){
    py_instance_mutex.lock();
    PyObject * result = NULL;
    result = PyObject_CallMethod(pInstance, TOOZ_WATCH_GROUP, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    int status = TOOZ_ERROR;
    if(result == NULL){
        PyErr_Print();
    }else{
        PyArg_Parse(result, "i", &status);
    }
    if(status == TOOZ_OK){
        start_callback_server_thread(group_id);
        start_run_watchers_thread();
    }
    return status;
}

//only used by sgclient, short connection
int ToozClient::refresh_nodes_view(string backend_url, string group_id){
    int ret = start_coordination(backend_url, "");
    if(ret == TOOZ_ERROR){
        LOG_ERROR << "ERROR start coordination, backend_url=" << backend_url << " group_id=" << group_id;
        return TOOZ_ERROR;
    }
    
    py_instance_mutex.lock();
    PyObject * result = NULL;
    result = PyObject_CallMethod(pInstance, TOOZ_REFRESH_NODES_VIEW, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    int status = TOOZ_ERROR;
    if(result == NULL){
        PyErr_Print();
    }else{
        PyArg_Parse(result, "i", &status);
    }
    ret = stop_coordination();
    if(ret == TOOZ_ERROR){
        LOG_WARN << "failed to stop coordination, backend_url=" << backend_url << " group_id=" << group_id;
    }
    return status;
}

int ToozClient::rehash_buckets_to_node(string group_id, int bucket_num){
    py_instance_mutex.lock();
    PyObject *p_bucket_list = PyObject_CallMethod(pInstance, TOOZ_REHASH_BUCKETS_TO_NODES,
                                                "(s,i)", group_id.c_str(), bucket_num);
    py_instance_mutex.unlock();
    old_buckets = new_buckets;
    new_buckets.clear();
    if(p_bucket_list){
        int size = PyList_GET_SIZE(p_bucket_list);
        int i;
        for(i=0; i<size; i++){
            PyObject *p_bucket = PyList_GetItem(p_bucket_list, i);
            string bucket = PyString_AsString(p_bucket);
            new_buckets.push_back(bucket);
        }
        return TOOZ_OK;
    }else{
        LOG_ERROR << "ERROR rehash bucket to node, bucket_num=" << bucket_num;
        return TOOZ_ERROR;
    }
}

list<string> ToozClient::get_buckets(){
    return new_buckets;
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

int ToozClient::get_cluster_size(string group_id){
    
    py_instance_mutex.lock();
    PyObject *member_list = PyObject_CallMethod(pInstance, TOOZ_GET_MEMBERS, "(s)", group_id.c_str());
    py_instance_mutex.unlock();
    if(member_list){
        return PyList_GET_SIZE(member_list);
    }else{
        return 0;
    }
}

int ToozClient::stop_coordination(){
    stop_heartbeat_thread();
    stop_run_watchers_thread();
    stop_callback_thread();
    py_instance_mutex.lock();
    PyObject * result = NULL;
    result = PyObject_CallMethod(pInstance, TOOZ_STOP, NULL);
    py_instance_mutex.unlock();
    int status = TOOZ_ERROR;
    if(result){
        PyArg_Parse(result, "i", &status);
    }
    return status;
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

int ToozClient::callback(char *event_type)
{
//TODO: use message queue
    if(0 == strncmp(event_type, TOOZ_JOIN_GROUP, sizeof(TOOZ_JOIN_GROUP))){
        for(auto & cb : callback_list){
            LOG_INFO << "join group callback";
            cb->join_group_callback(old_buckets, new_buckets);
        }
    }else{
        for(auto & cb : callback_list){
            LOG_INFO << "leave group callback";
            cb->leave_group_callback(old_buckets, new_buckets);
        }
    }
}

void ToozClient::register_callback(ICallback *callback){
    callback_list.push_back(callback);
}

void ToozClient::unregister_callback(ICallback *callback){
    callback_list.remove(callback);
}

void ToozClient::callback_server(string group_id){
    struct sockaddr_un address;
    int socket_fd, connection_fd;
    socklen_t address_length;
    pid_t child;
    LOG_INFO << "start callback server";
    socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
    if(socket_fd < 0){
        LOG_ERROR << "socket() failed";
        return ;
    }
    unlink(SOCKET_FILE);
    memset(&address, 0, sizeof(struct sockaddr_un));
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, SUN_PATH_MAX, "%s", SOCKET_FILE);
    if(bind(socket_fd,
         (struct sockaddr *) &address,
         sizeof(struct sockaddr_un)) != 0){
        LOG_ERROR << "bind() failed";
        return ;
    }
    if(listen(socket_fd, 5) != 0){
        LOG_ERROR << "listen() failed";
        return ;
    }
    if((connection_fd = accept(socket_fd,
                               (struct sockaddr *) &address,
                               &address_length)) > -1){
        while(terminate_callback){
            int nbytes;
            char event_type[BUF_MAX_LEN];
            nbytes = read(connection_fd, event_type, BUF_MAX_LEN);
            event_type[nbytes] = 0;
            LOG_DEBUG << "get one message from tooz callback";
            LOG_DEBUG << "event_type=" << event_type;
            rehash_buckets_to_node(group_id);
            callback(event_type);
        }
    }
    close(connection_fd);
    close(socket_fd);
    unlink(SOCKET_FILE);
    return ;
}

