#include <set>
#include <string>
#include <iostream>
//#include <boost/test/unit_test.hpp> // boost.test header
#include "../rpc/clients/snapshot_ctrl_client.h"

using namespace std;

//BOOST_AUTO_TEST_SUITE(control_client_test)
//    BOOST_AUTO_TEST_CASE(case1){ 

int main(int argc, char** argv)
{
    int ret     = 0;
    char* op    = argv[1];

    SnapCtrlClient* snap_client = new SnapCtrlClient(grpc::CreateChannel(
                "127.0.0.1:1111", 
                grpc::InsecureChannelCredentials()));

    string vol_name  = "test_volume";

    if(strcmp(op, "create") == 0){
        int snap_id = atoi(argv[2]);
        string snap_name = "test_volume_snap_" + to_string(snap_id);
        ret = snap_client->CreateSnapshot(vol_name, snap_name);
        cout << "create snapshot " << "snap_name:" << snap_name << " ret:" << ret << endl;
    } else if(strcmp(op, "delete") == 0){
        int snap_id = atoi(argv[2]);
        string snap_name = "test_volume_snap_" + to_string(snap_id);
        ret = snap_client->DeleteSnapshot(vol_name, snap_name);
        cout << "delete snapshot " << "snap_name:" << snap_name << " ret:" << ret << endl;
    } else if(strcmp(op, "rollback") == 0){
        int snap_id = atoi(argv[2]);
        string snap_name = "test_volume_snap_" + to_string(snap_id);
        ret = snap_client->RollbackSnapshot(vol_name, snap_name);
        cout << "roll snapshot " << "snap_name:" << snap_name << " ret:" << ret << endl;
    } else if(strcmp(op, "diff") == 0){
        int first_snap_id = atoi(argv[2]);
        int last_snap_id  = atoi(argv[3]);
        string first_snap_name = "test_volume_snap_" + to_string(first_snap_id);
        string last_snap_name  = "test_volume_snap_" + to_string(last_snap_id);
        ret = snap_client->DiffSnapshot(vol_name, first_snap_name, last_snap_name);
        cout << "diff snapshot " << "first_snap_name:" << first_snap_name 
             << " last_snap_name:" << last_snap_name << " ret:" << ret << endl;
    } else if(strcmp(op, "read") == 0){
        int snap_id = atoi(argv[2]);
        off_t  len  = atoi(argv[3]);
        size_t off  = atoi(argv[4]);

        string snap_name = "test_volume_snap_" + to_string(snap_id);
        char* buf = (char*)malloc(len);

        ret = snap_client->ReadSnapshot(vol_name, snap_name, buf, len, off);
        cout << "read snapshot " 
             << " snap_name:" << snap_name 
             << " off:" << off
             << " len:" << len
             << " ret:" << ret << endl;
    } else if(strcmp(op, "query") == 0){
        int snap_id = atoi(argv[2]);
        string snap_name = "test_volume_snap_" + to_string(snap_id);
        SnapStatus snap_status;
        ret = snap_client->QuerySnapshot(vol_name, snap_name, snap_status);
        cout << "query snapshot: "  << snap_name << " status:" << snap_status << endl;
    }

    //snap_name = "test_volume_snap_1";
    //ret = snap_client->CreateSnapshot(vol_name, snap_name);
    //cout << "create snapshot1 ret:" << ret << endl;

    //set<string> snap_set;
    //ret = snap_client->ListSnapshot(vol_name, snap_set);        
    //cout << "list snapshot1 ret:" << ret << endl;
    //for(auto it : snap_set){
    //    cout << it << endl;
    //}

    //snap_name = "test_volume_snap_1";
    //ret = snap_client->DeleteSnapshot(vol_name, snap_name);
    //cout << "delete snapshot1 ret:" << ret << endl;

    //snap_set.clear();
    //ret = snap_client->ListSnapshot(vol_name, snap_set);        
    //cout << "list snapshot2 ret:" << ret << endl;
    //for(auto it : snap_set){
    //    cout << it << endl;
    //}
    return 0;
}
        //BOOST_REQUIRE(res == DRS_OK);
//    }

//BOOST_AUTO_TEST_SUITE_END()

