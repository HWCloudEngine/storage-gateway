#include <string>
#include <stdio.h>
#include <iostream>
#include <list>
#include <algorithm>
#include "tooz_client.h"
#include "simple_callback.h"
#include "log.h"
using namespace std;
struct print{
    int count;
    print(){count = 0;}
    void operator()(string x)
    {
        printf("%s,",x.c_str());
        ++count;
    }
};

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
    //list<string> nodes;
    
    //cout << "get_node:" << sg_client.get_node("1") << endl;
    //sg_client.refresh_nodes_view(backend_url, group_id);
    //cout << "get_node:" << sg_client.get_node("1") << endl;
    SimpleCallback *callback = new SimpleCallback();
    SimpleCallback *callback2 = new SimpleCallback();
    tc.register_callback(callback);
    tc.register_callback(callback2);
    //print p = for_each(tc.get_buckets().begin(), tc.get_buckets().end(), print());
    //cout << tc.get_node_id() << "takes control buckets num : " << p.count << endl;
    sleep(60);
    tc.stop_coordination();
    free(callback);
    free(callback2);
    return 0;
}
