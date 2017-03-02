#ifndef BACKUP_TASK_H
#define BACKUP_TASK_H
#include <memory>
#include "rpc/common.pb.h"
#include "transfer/net_sender.h"
#include "backup_ctx.h"

using namespace std;
using huawei::proto::StatusCode;

class AsyncTask
{
public:
    enum TaskStatus 
    {
        TASK_CREATE  = 0,
        TASK_READY   = 1,
        TASK_RUN     = 2,
        TASK_SUSPEND = 3,
        TASK_RESUME  = 4,
        TASK_DONE    = 5,
        TASK_ERROR   = 6,
    };

    enum TaskType
    {
        BACKUP_CREATE = 1,
        BACKUP_DELETE = 2,
    };

	AsyncTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
	virtual ~AsyncTask();

    string task_name()const {
        return m_task_name;
    }

    virtual bool ready() = 0;
	virtual void work()  = 0;

    bool finish() const {
        return m_task_status == TASK_DONE ? true : false; 
    }

protected:
    /*current task operate on which backup*/
    string m_backup_name;
    /*backup up global contex*/
	shared_ptr<BackupCtx> m_ctx;

    string     m_task_name;
    TaskType   m_task_type;
	TaskStatus m_task_status;
};

class IBackupCreate
{
protected:
    /*overide by local and remote backup create*/
    virtual StatusCode do_full_backup() = 0;
    virtual StatusCode do_incr_backup() = 0;
};

class LocalCreateTask : public AsyncTask, public IBackupCreate
{
public:
	explicit LocalCreateTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
	~LocalCreateTask();

    bool ready() override;
	void work()  override;

protected:
    StatusCode do_full_backup() override;
    StatusCode do_incr_backup() override;
};

class RemoteCreateTask : public LocalCreateTask
{
public:
	explicit RemoteCreateTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
	~RemoteCreateTask();

    bool ready() override;

protected:
    StatusCode do_full_backup() override;
    StatusCode do_incr_backup() override;
private:
    StatusCode remote_create_start();
    StatusCode remote_create_upload(block_t blk_no, off_t blk_off, char* blk_data, size_t blk_data_len);
    StatusCode remote_create_end();
private:
    grpc_stream_ptr m_remote_stream;
};

class IBackupDelete
{
protected:
    /*overide by local and remote backup delete*/
    virtual StatusCode do_delete_backup(const string& cur_backup) = 0;
    virtual StatusCode do_merge_backup(const string& cur_backup, const string& next_backup) = 0;
};

class LocalDeleteTask : public AsyncTask, public IBackupDelete
{
public:
	explicit LocalDeleteTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
	~LocalDeleteTask();

    bool ready() override;
	void work()  override;
    
private:
    /*backup has no depended*/
    StatusCode do_delete_backup(const string& cur_backup) override;
    /*backup has depended*/
    StatusCode do_merge_backup(const string& cur_backup, const string& next_backup) override;
};

class RemoteDeleteTask : public LocalDeleteTask
{
public:
	explicit RemoteDeleteTask(const string& backup_name, shared_ptr<BackupCtx> ctx);
	~RemoteDeleteTask();

private:
    StatusCode remote_delete();
	void work()  override;
private:
    grpc_stream_ptr m_remote_stream;
};

#endif
