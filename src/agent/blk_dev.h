#ifndef BLK_DEV_H
#define BLK_DEV_H

#include <linux/types.h>
#include <linux/spinlock.h>
#include <linux/fs.h>
#include <linux/blkdev.h>
#include <linux/sched.h>
#include <linux/wait.h>
#include <linux/completion.h>
#include <scsi/scsi_device.h>
#include "transport.h"

struct pbdev
{
    spinlock_t       lock;
    struct list_head link;

    char*                 vol_name;
    char*                 blk_path;
    struct block_device*  blk_device;
    struct request_queue* blk_queue;
    struct scsi_device*   scsi_device;
    spinlock_t            blk_queue_lock;
    request_fn_proc*      blk_request_fn;
    make_request_fn*      blk_bio_fn;
    
    /*each bit in cbt bitmap represent block size(2^n)*/
    size_t                granularity;
    size_t                granularity_shit;
    /*unit byte*/
    size_t                cbt_bitmap_size;
    /*trace which sectors still in sg_client side(one bit one sector)*/
    unsigned long*        cbt_bitmap;

    struct transport*     network;
    
    uint64_t              seq_id;

    wait_queue_head_t     send_wq;
    struct bio_list       send_bio_list;
    struct task_struct*   send_thread;
    wait_queue_head_t     recv_wq;
    struct bio_list       recv_bio_list;
    struct task_struct*   recv_thread;
    
    /*add and delete volume synchronize*/
    struct completion cmd_sync_event;
};

int blk_dev_protect(const char* dev_path, const char* vol_name);
int blk_dev_unprotect(const char* dev_path);

struct pbdev_mgr
{
    char* sg_host;
    int   sg_port;
    pid_t sg_pid;
    spinlock_t dev_lock;
    struct list_head dev_list;
};

/*protect block device management*/
extern struct pbdev_mgr g_dev_mgr;

int  pbdev_mgr_init(struct pbdev_mgr* dev_mgr);
void pbdev_mgr_fini(struct pbdev_mgr* dev_mgr);

#endif
