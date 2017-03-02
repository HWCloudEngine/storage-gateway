#ifndef BACKUP_TASK_H
#define BACKUP_TASK_H
#include <memory>
#include "rpc/common.pb.h"
#include "backup_ctx.h"

using namespace std;
using huawei::proto::StatusCode;

enum BackupTaskStatus 
{
    TASK_READY   = 1,
    TASK_RUN     = 2,
    TASK_SUSPEND = 3,
    TASK_RESUME  = 4,
    TASK_DONE    = 5,
    TASK_ERROR   = 6,
};
using BackupTaskStatus = enum BackupTaskStatus;

class BackupTask
{
public:
    BackupTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
    virtual ~BackupTask();

    string task_name()const{return m_task_name;}
    BackupTaskStatus task_status()const{return m_task_status;}

    virtual void work() = 0;

protected:
    /*current task operate on which backup*/
    string m_backup_name;
    /*backup up global contex*/
    shared_ptr<BackupCtx> m_ctx;

    string                m_task_name;
    enum BackupTaskStatus m_task_status;
};

class CreateBackupTask : public BackupTask
{
public:
    explicit CreateBackupTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
    ~CreateBackupTask();

    void work() override;

private:
    StatusCode do_full_backup();
    StatusCode do_incr_backup();
};

class DeleteBackupTask : public BackupTask
{
public:
    explicit DeleteBackupTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
    ~DeleteBackupTask();

    void work() override;
    
private:
    /*backup has no depended*/
    StatusCode do_delete_backup(const string& cur_backup);
    /*backup has depended*/
    StatusCode do_merge_backup(const string& cur_backup, const string& next_backup);
};

#endif
