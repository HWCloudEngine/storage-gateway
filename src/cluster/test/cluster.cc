#include <string>
#include <stdio.h>
#include <iostream>
#include <list>
#include <algorithm>
#include "tooz_client.h"
#include "simple_callback.h"
#include "log.h"
using namespace std;

int main(int argc, char *argv[]){
    ToozClient tc;
    ToozClient sg_client;
    string backend_url = "kazoo://162.3.111.245:2181";
    string group_id = "sg_new_group";
    tc.start_coordination(backend_url, "node1");
    tc.join_group(group_id);
    tc.watch_group(group_id);
    tc.rehash_buckets_to_node(group_id, 10);
    LOG_INFO << "multi_instance start";

    SimpleCallback callback;
    tc.register_callback(&callback);
    sleep(60);
    tc.stop_coordination();
    return 0;
}
