#include <set>
#include <string>
#include <iostream>
#include "common/utils.h"
#include "../log/log.h"
#include "../rpc/clients/volume_ctrl_client.h"
#include "../rpc/clients/snapshot_ctrl_client.h"
#include "../rpc/clients/backup_ctrl_client.h"
#include "common/config_option.h"
#include "common/utils.h"

using namespace std;

int main(int argc, char** argv)
{
    int ret = 0;

    DRLog::log_init("sg_tool.log");
    
    std::string rpc_addr = rpc_address(g_option.ctrl_server_ip, g_option.ctrl_server_port);
    VolumeCtrlClient* vol_client = new VolumeCtrlClient(grpc::CreateChannel(
                rpc_addr, 
                grpc::InsecureChannelCredentials()));
    BackupCtrlClient* backup_client = new BackupCtrlClient(grpc::CreateChannel(
                rpc_addr, 
                grpc::InsecureChannelCredentials()));
    SnapshotCtrlClient* snap_client = new SnapshotCtrlClient(grpc::CreateChannel(
                rpc_addr, 
                grpc::InsecureChannelCredentials()));

    string vol_name = "test_volume";
    size_t vol_size = 16777216UL;
    string blk_device = "/dev/sdb";
    
    /*"volume", "snapshot", "backup"*/
    char* object = argv[1];
    
    if(strcmp(object, "volume") == 0){
        char* op = argv[2];
        int mode = atoi(argv[3]);
        if(strcmp(op, "enable") == 0){
            ret = vol_client->enable_sg(vol_name, vol_size, blk_device);
            ret = vol_client->attach_volume(mode, vol_name, blk_device, "127.0.0.1");
            cout << "enable sg " << "vol_name:" << vol_name << " ret:" << ret << endl;
        } else if(strcmp(op, "disable") == 0){
            ret = vol_client->detach_volume(mode, vol_name, blk_device);
            ret = vol_client->disable_sg(vol_name);
            cout << "disable sg" << "vol_name:" << vol_name << " ret:" << ret << endl;
        } 
    }
    
    if(strcmp(object, "snapshot") == 0){
        char* op = argv[2];
        if(strcmp(op, "create") == 0){
            string snap_name = argv[3];
            ret = snap_client->CreateSnapshot(vol_name, snap_name);
            cout << "create snapshot " << "snap_name:" << snap_name << " ret:" << ret << endl;
        } else if(strcmp(op, "delete") == 0){
            string snap_name = argv[3];
            ret = snap_client->DeleteSnapshot(vol_name, snap_name);
            cout << "delete snapshot " << "snap_name:" << snap_name << " ret:" << ret << endl;
        } else if(strcmp(op, "rollback") == 0){
            string snap_name = argv[3];
            ret = snap_client->RollbackSnapshot(vol_name, snap_name);
            cout << "roll snapshot " << "snap_name:" << snap_name << " ret:" << ret << endl;
        } else if(strcmp(op, "diff") == 0){
            string first_snap_name = argv[3];
            string last_snap_name  = argv[4];
            vector<DiffBlocks> diff;
            ret = snap_client->DiffSnapshot(vol_name, first_snap_name, last_snap_name, diff);
            cout << "diff snapshot " << "first_snap_name:" << first_snap_name 
                << " last_snap_name:" << last_snap_name << " ret:" << ret << endl;
            for (auto blocks : diff) {
                cout << "diff snap:" << blocks.snap_name() << endl;
                int block_num = blocks.block_size();
                for (int i = 0; i < block_num; i++) {
                    cout << "\t" << blocks.block(i).blk_no() << " "
                         << blocks.block(i).blk_zero() << " " << blocks.block(i).blk_url();
                }
                cout << endl;
            }
        } else if(strcmp(op, "read") == 0){
            string snap_name = argv[3];
            off_t  off = atoi(argv[4]);
            size_t len = atoi(argv[5]);
            char*  buf = (char*)malloc(len);
            ret = snap_client->ReadSnapshot(vol_name, snap_name, buf, len, off);
            string file = "/opt/" + snap_name + to_string(off) + to_string(len);
            save_file(file, buf, len);
            free(buf);
            cout << "read snapshot " << " snap_name:" << snap_name << " off:" << off
                 << " len:" << len << " ret:" << ret << endl;
        } else if(strcmp(op, "query") == 0){
            string snap_name = argv[3];
            SnapStatus snap_status;
            ret = snap_client->QuerySnapshot(vol_name, snap_name, snap_status);
            cout << "query snapshot: "  << snap_name << " status:" << snap_status << endl;
        } else if(strcmp(op, "restore_volume") == 0) {
            string snap_name = argv[3];
            string new_vol = argv[4];
            string new_blk = argv[5];
            ret = snap_client->CreateVolumeFromSnap(vol_name, snap_name, new_vol,new_blk);
            cout << "restore volume ok";
        
        } else if(strcmp(op, "query_volume") == 0) {
            string new_vol = argv[3];
            VolumeStatus status;
            ret = snap_client->QueryVolumeFromSnap(new_vol, status);
            cout << "restore volume status:" << status;
        }
    }
    
    if(strcmp(object, "backup") == 0){
        char* op = argv[2];
        if(strcmp(op, "create") == 0){
            string backup_name = argv[3];
            BackupType backup_type = (BackupType)atoi(argv[4]);
            BackupMode backup_mode = (BackupMode)atoi(argv[5]);
            BackupOption backup_option;
            backup_option.set_backup_mode(backup_mode);
            backup_option.set_backup_type(backup_type);
            ret = backup_client->CreateBackup(vol_name, vol_size, backup_name, backup_option);
            cout << "create backup " << "backup_name:" << backup_name << " ret:" << ret << endl;
        } else if(strcmp(op, "delete") == 0){
            string backup_name = argv[3];
            ret = backup_client->DeleteBackup(vol_name, backup_name);
            cout << "delete backup " << "backup_name:" << backup_name << " ret:" << ret << endl;
        } else if(strcmp(op, "restore") == 0){
            string backup_name = argv[3];
            BackupType backup_type = (BackupType)atoi(argv[4]);
            string new_vol_name = argv[5];
            size_t new_vol_size = atoi(argv[6]);
            string new_block_deive = argv[7];
            ret = backup_client->RestoreBackup(vol_name, backup_name, backup_type, new_vol_name, new_vol_size, new_block_deive);
            cout << "restore backup " << "backup_name:" << backup_name << " ret:" << ret << endl;
        } else if(strcmp(op, "list") == 0){
            set<string> backup_set;
            ret = backup_client->ListBackup(vol_name, backup_set);
            cout << "list backup " << "volume_name:" << vol_name << " ret:" << ret << endl;
        } else if(strcmp(op, "get") == 0){
            string backup_name = argv[3];
            BackupStatus backup_status;
            ret = backup_client->GetBackup(vol_name, backup_name, backup_status);
            cout << "get backup " << " backup_name:" << backup_name 
                << " backup_status:" << backup_status << " ret:" << ret << endl;
        } 
    }

    return 0;
}
