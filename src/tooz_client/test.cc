#include <string>
#include <stdio.h>
#include "tooz_client.h"

using namespace std;
int main(int argc, char *argv[]){
	ToozClient tc;
	string backend_url = "kazoo://162.3.111.245:2181";
	string group_id = "sg_new_group";
	tc.start_coordination(backend_url, "member1");
	tc.join_group(group_id);
	tc.watch_group(group_id);
	tc.stop_coordination();
}
