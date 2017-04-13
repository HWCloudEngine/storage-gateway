#ifndef BLK_DEV_H
#define BLK_DEV_H

#include <linux/types.h>
#include <linux/spinlock.h>
#include <linux/fs.h>
#include <linux/blkdev.h>
#include <linux/sched.h>
#include <linux/wait.h>
#include <scsi/scsi_device.h>
#include "transport.h"

struct pbdev
{
    spinlock_t       lock;
    struct list_head link;
    
    char*                 blk_path;
    struct block_device*  blk_device;
    struct request_queue* blk_queue;
    struct scsi_device*   scsi_device;
    spinlock_t            blk_queue_lock;
    request_fn_proc*      blk_request_fn;
    make_request_fn*      blk_bio_fn;
    
    struct transport*     network;

    wait_queue_head_t     send_wq;
    struct list_head      send_queue;
    struct bio_list       send_bio_list;
    struct task_struct*   send_thread;
    wait_queue_head_t     recv_wq;
    struct list_head      recv_queue;
    struct bio_list       recv_bio_list;
    struct task_struct*   recv_thread;
};

int blk_dev_protect(const char* dev_path);
int blk_dev_unprotect(const char* dev_path);

struct pbdev_mgr
{
	char* sg_host;
	int   sg_port;
	pid_t sg_pid;
    spinlock_t       dev_lock;
    struct list_head dev_list;
};

/*protect block device management*/
extern struct pbdev_mgr g_dev_mgr;

int  pbdev_mgr_init(struct pbdev_mgr* dev_mgr);
void pbdev_mgr_fini(struct pbdev_mgr* dev_mgr);
int pbdev_mgr_add(struct pbdev_mgr* dev_mgr, struct pbdev* dev);
int pbdev_mgr_del(struct pbdev_mgr* dev_mgr, const char* dev);
struct pbdev* pbdev_mgr_get_by_path(struct pbdev_mgr* dev_mgr, const char* blk_path);
struct pbdev* pbdev_mgr_get_by_queue(struct pbdev_mgr* dev_mgr, struct request_queue* q);

#endif
