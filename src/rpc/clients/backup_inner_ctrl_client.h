#ifndef BACKUP_INNER_CTRL_CLIENT_H_
#define BACKUP_INNER_CTRL__CLIENT_H_
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <fstream>
#include <memory>
#include <set>
#include <vector>
#include <grpc++/grpc++.h>
#include "../backup.pb.h"
#include "../backup_inner_control.pb.h"
#include "../backup_inner_control.grpc.pb.h"
#include "common/define.h"
#include "common/block_store.h"

using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;
using huawei::proto::StatusCode;
using huawei::proto::BackupMode;
using huawei::proto::BackupStatus;
using huawei::proto::BackupOption;

using huawei::proto::inner::BackupInnerControl;
using huawei::proto::inner::CreateBackupInReq;
using huawei::proto::inner::CreateBackupInAck;
using huawei::proto::inner::ListBackupInReq;
using huawei::proto::inner::ListBackupInAck;
using huawei::proto::inner::GetBackupInReq;
using huawei::proto::inner::GetBackupInAck;
using huawei::proto::inner::DeleteBackupInReq;
using huawei::proto::inner::DeleteBackupInAck;
using huawei::proto::inner::RestoreBackupInReq;
using huawei::proto::inner::RestoreBackupInAck;

/*backup control rpc client*/
class BackupInnerCtrlClient {
 public:
    explicit BackupInnerCtrlClient(shared_ptr<Channel> channel)
        :m_stub(BackupInnerControl::NewStub(channel)) {
    }

    ~BackupInnerCtrlClient() {
    }

    StatusCode CreateBackup(const string& vol_name,
                            const size_t& vol_size,
                            const string& backup_name,
                            const BackupOption& backup_option) {
        CreateBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_vol_size(vol_size);
        req.set_backup_name(backup_name);
        req.mutable_backup_option()->CopyFrom(backup_option);
        CreateBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Create(&context, req, &ack);
        return ack.status();
    }

    StatusCode ListBackup(const string& vol_name, set<string>& backup_set) {
        ListBackupInReq req;
        req.set_vol_name(vol_name);
        ListBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->List(&context, req, &ack);
        cout << "ListBackup size:" << ack.backup_name_size() << endl;
        for (int i = 0; i < ack.backup_name_size(); i++) {
            cout << "ListBackup backup:" << ack.backup_name(i) << endl;
            backup_set.insert(ack.backup_name(i));
        }
        return ack.status();
    }

    StatusCode GetBackup(const string& vol_name, const string& backup_name,
                         BackupStatus& backup_status) {
        GetBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        GetBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Get(&context, req, &ack);
        backup_status = ack.backup_status();
        return ack.status();
    }

    StatusCode DeleteBackup(const string& vol_name, const string& backup_name) {
        DeleteBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        DeleteBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Delete(&context, req, &ack);
        return ack.status();
    }

    StatusCode RestoreBackup(const string& vol_name,
                             const string& backup_name,
                             const string& new_vol_name,
                             const size_t& new_vol_size,
                             const string& new_block_device,
                             BlockStore* block_store) {
        RestoreBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        RestoreBackupInAck ack;
        ClientContext context;
        unique_ptr<ClientReader<RestoreBackupInAck>> reader(m_stub->Restore(&context, req));

        int block_dev_fd = open(new_block_device.c_str(), O_WRONLY|O_SYNC);
        assert(block_dev_fd != -1);
        char* buf = new char[BACKUP_BLOCK_SIZE];
        assert(buf != nullptr);

        while (reader->Read(&ack) && !ack.blk_over()) {
            uint64_t blk_no = ack.blk_no();
            string blk_obj = ack.blk_obj();
            char* blk_data = (char*)ack.blk_data().c_str();

            LOG_INFO << "restore blk_no:" << blk_no << " blk_oj:" << blk_obj
                     << " blk_data_len:" << ack.blk_data().length();

            if (!blk_obj.empty()) {
                /*(local)read from block store*/
                int read_ret = block_store->read(blk_obj, buf, BACKUP_BLOCK_SIZE, 0);
                assert(read_ret == BACKUP_BLOCK_SIZE);
                blk_data = buf;
            }

            if (blk_data) {
                /*write to new block device*/
                int write_ret = pwrite(block_dev_fd, blk_data,
                                       BACKUP_BLOCK_SIZE,
                                       blk_no * BACKUP_BLOCK_SIZE);
                assert(write_ret == BACKUP_BLOCK_SIZE);
            }
        }
        Status status = reader->Finish();
        if (buf) {
            delete [] buf;
        }
        if (block_dev_fd != -1) {
            close(block_dev_fd);
        }
        return status.ok() ? StatusCode::sOk : StatusCode::sInternalError;
    }

 private:
    unique_ptr<BackupInnerControl::Stub> m_stub;
};

#endif
