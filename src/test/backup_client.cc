#include <set>
#include <string>
#include <iostream>
//#include <boost/test/unit_test.hpp> // boost.test header
#include "../rpc/clients/backup_ctrl_client.h"

using namespace std;

//BOOST_AUTO_TEST_SUITE(control_client_test)
//    BOOST_AUTO_TEST_CASE(case1){ 

int main(int argc, char** argv)
{
    int ret     = 0;
    char* op    = argv[1];

    BackupCtrlClient* backup_client = new BackupCtrlClient(grpc::CreateChannel(
                                    "127.0.0.1:1111", 
                                    grpc::InsecureChannelCredentials()));

    string vol_name  = "test_volume";
    size_t vol_size  = 4 * 1024 * 1024 * 1024UL;

    if(strcmp(op, "create") == 0){
        string backup_name = argv[2];
        int backup_mode = atoi(argv[3]);
        BackupOption backup_option;
        backup_option.set_backup_mode(backup_mode ? BackupMode::BACKUP_FULL : BackupMode::BACKUP_INCR);
        ret = backup_client->CreateBackup(vol_name, vol_size, backup_name, backup_option);
        cout << "create backup " << "backup_name:" << backup_name << " ret:" << ret << endl;
    } else if(strcmp(op, "delete") == 0){
        string backup_name = argv[2];
        ret = backup_client->DeleteBackup(vol_name, backup_name);
        cout << "delete backup " << "backup_name:" << backup_name << " ret:" << ret << endl;
    } else if(strcmp(op, "restore") == 0){
        string backup_name = argv[2];
        string new_vol_name = argv[3];
        size_t new_vol_size = atoi(argv[4]);
        string new_block_deive = argv[5];
        ret = backup_client->RestoreBackup(vol_name, backup_name, new_vol_name, new_vol_size, new_block_deive);
        cout << "restore backup " << "backup_name:" << backup_name << " ret:" << ret << endl;
    } else if(strcmp(op, "list") == 0){
        set<string> backup_set;
        ret = backup_client->ListBackup(vol_name, backup_set);
        cout << "list backup " << "volume_name:" << vol_name << " ret:" << ret << endl;
    } else if(strcmp(op, "query") == 0){
        string backup_name = argv[2];
        BackupStatus backup_status;
        ret = backup_client->QueryBackup(vol_name, backup_name, backup_status);
        cout << "query backup " << " backup_name:" << backup_name 
             << " backup_status:" << backup_status << " ret:" << ret << endl;
    } 

    return 0;
}
        //BOOST_REQUIRE(res == DRS_OK);
//    }

//BOOST_AUTO_TEST_SUITE_END()

