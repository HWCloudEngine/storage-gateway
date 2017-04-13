#include <linux/kthread.h>
#include <linux/string.h>
#include <linux/blkdev.h>
#include <linux/blk_types.h>
#include <linux/sched.h>
#include <linux/delay.h>
#include <linux/pid.h>
#include <asm/barrier.h>
#include <scsi/scsi_host.h>
#include <scsi/scsi_cmnd.h>
#include <scsi/scsi_device.h>
#include "log.h"
#include "message.h"
#include "blk_dev.h"
#include <linux/highmem.h>

struct pbdev_mgr g_dev_mgr;

void hook_request_fn(struct request_queue* q)
{
    struct request* req;
    struct pbdev* dev;

    while((req = blk_fetch_request(q)) != NULL){
        spin_unlock_irq(q->queue_lock);
        dev = pbdev_mgr_get_by_queue(&g_dev_mgr, q);

        /*todo check network status*/
        spin_lock_irq(&dev->blk_queue_lock);
        list_add_tail(&req->queuelist, &dev->send_queue);
        /*wakup network send thread*/
        spin_unlock_irq(&dev->blk_queue_lock);
        wake_up(&dev->send_wq);
        spin_lock_irq(q->queue_lock);
    }
}

void hook_make_request_fn(struct request_queue* q, struct bio* bio)
{
    struct pbdev* dev = NULL;
    int pass = 0;
    pid_t cur_tgid = current->tgid;
    dev = pbdev_mgr_get_by_queue(&g_dev_mgr, q);
    if(dev == NULL){
        LOG_ERR("get dev by queue failed");
        return;
    }
    
    /* sg client bio */
    spin_lock_irq(&g_dev_mgr.dev_lock);
    pass = ((cur_tgid == g_dev_mgr.sg_pid)?1:0);
    spin_unlock_irq(&g_dev_mgr.dev_lock);
    if(pass)
    {
        dev->blk_bio_fn(q,bio);
        return;
    }

    spin_lock_irq(&dev->lock);
    bio_list_add(&dev->send_bio_list, bio);
    spin_unlock_irq(&dev->lock);
    /*wakup network send thread*/
    wake_up(&dev->send_wq);
}

int hook_queuecommand_fn(struct Scsi_Host* host, struct scsi_cmnd* cmd)
{
    if(cmd) cmd->scsi_done(cmd);
    return 0;
}

static int install_hook(struct pbdev* dev, 
                        request_fn_proc* new_request_fn,
                        make_request_fn* new_make_request_fn)
{
    int ret = 0;
    struct super_block* sb = dev->blk_device->bd_super;
    if(sb){
        LOG_INFO("freezing block device");
        sb = freeze_bdev(dev->blk_device);
        if(!sb){
            LOG_ERR("freeze bdev failed");
            return -EFAULT;
        }
        if(IS_ERR(sb)){
            ret = PTR_ERR(sb);
            LOG_ERR("freeze bdev failed:%d", ret);
            return ret;
        }
        LOG_INFO("freezing block device ok");
    }
    smp_wmb(); 
    //dev->blk_device->bd_disk->queue->request_fn = new_request_fn;
    dev->blk_device->bd_disk->queue->make_request_fn = new_make_request_fn;
    //dev->blk_device->bd_disk->queue->prep_rq_fn = hook_prep_rq_fn;
    smp_wmb(); 
    if(sb){
        ret = thaw_bdev(dev->blk_device, sb);
        if(ret){
            LOG_ERR("thaw bdev failed:%d", ret); 
            return ret;
        }
        LOG_INFO("thrawing block device ok");
    }
    return ret;
}

static int uninstall_hook(struct pbdev* dev)
{
    int ret = 0;
    struct super_block* sb = dev->blk_device->bd_super;
    if(sb){
        LOG_INFO("freezing block device");
        sb = freeze_bdev(dev->blk_device);
        if(!sb){
            LOG_ERR("freeze bdev failed");
            return -EFAULT;
        }
        if(IS_ERR(sb)){
            ret = PTR_ERR(sb);
            LOG_ERR("free bdev failed:%d", ret);
            return ret;
        }
    }
    smp_wmb(); 
    //dev->blk_device->bd_disk->queue->request_fn = dev->blk_request_fn;
    dev->blk_device->bd_disk->queue->make_request_fn = dev->blk_bio_fn;
    smp_wmb(); 
    if(sb){
        LOG_INFO("thrawing block device");
        ret = thaw_bdev(dev->blk_device, sb);
        if(ret){
            LOG_ERR("thaw bdev failed:%d", ret); 
        }
    }
    return ret;
}

static int net_send_vol_ctl_cmd(struct pbdev* dev,bool add_vol)
{
    int ret = 0;
    struct HookReply reply = {0};
    struct HookRequest* req = NULL;
    int req_len = sizeof(struct HookRequest);
    req_len += add_vol ? sizeof(add_vol_req_t) : sizeof(del_vol_req_t);

    req = kzalloc(req_len, GFP_KERNEL);
    if(req == NULL)
    {
        LOG_ERR("allocte memory failed");
        ret = -1;
        goto out;
    }
    req->magic = MSG_MAGIC;
    req->type  = add_vol ? ADD_VOLUME : DEL_VOLUME;
    req->reserves = 0;
    req->handle = 0;
    req->offset = 0;
    req->len = add_vol ? sizeof(add_vol_req_t) : sizeof(del_vol_req_t);
   
    if(add_vol){
        add_vol_req_t* add_vol = (add_vol_req_t*)req->data;
        strcpy(add_vol->vol_name, dev->vol_name);
        strcpy(add_vol->dev_path, dev->blk_path);
    } else {
        del_vol_req_t* del_vol = (del_vol_req_t*)req->data;
        strcpy(del_vol->vol_name, dev->vol_name);
    }

    ret = tp_send(dev->network, (char*)req, req_len);
    if(ret != req_len){
        LOG_ERR("vol cmd send err ret:%d size:%d ", ret, req_len);
        goto out;;
    }

    ret = tp_recv(dev->network, (char*)&reply, sizeof(reply));
    if(ret != sizeof(reply)){
        LOG_ERR("vol cmd recv err ret:%d size:%ld", ret, sizeof(reply));
        goto out;
    }
    
    if(reply.error == 0){
        LOG_ERR("%s ok", (add_vol ? "Add vol" : "Del vol"));
        ret = 0;
    } else {
        LOG_ERR("%s failed", (add_vol ? "Add vol" : "Del vol"));
        ret = -1;
    }
    goto out;

out:
    if(req){
        kfree(req);
    }
    return ret;;


}

static int net_send_bvec(struct pbdev* dev, struct bio_vec* bvec)
{
    int ret;
    void* kaddr = kmap(bvec->bv_page);
    ret = tp_send(dev->network, kaddr + bvec->bv_offset, bvec->bv_len);
    kunmap(kaddr);
    return ret;
}

static int net_send_req(struct pbdev* dev, struct request* req)
{
    int ret;
    uint64_t size = blk_rq_bytes(req);
    uint64_t off  = (blk_rq_pos(req) << 9); 
    //uint8_t  dir  = req->cmd[0]; /*READ=0 WRITE=1*/
    uint8_t dir = rq_data_dir(req);

    struct HookRequest hreq;
    hreq.magic = MSG_MAGIC;
    hreq.type  = dir ? IO_WRITE : IO_READ;
    hreq.handle = (uint64_t)req;
    hreq.offset = off;
    hreq.len = size;
    ret = tp_send(dev->network, (const char*)&hreq, sizeof(hreq));
    if(ret){
        LOG_ERR("send req failed ret:%d len:%lu", ret, sizeof(hreq));
        return ret;
    }
    /*send write io data*/
    if(rq_data_dir(req) == WRITE){
        struct req_iterator iter;
        struct bio_vec*     bvec;
        rq_for_each_segment(bvec, req, iter) {
            ret = net_send_bvec(dev, bvec);
            if(ret){
                LOG_ERR("send bvec failed ret:%d len:%d", ret, bvec->bv_len);
                return ret;
            }
        }
    }
    return 0;
}

static int net_send_bio(struct pbdev* dev, struct bio* bio)
{
    int ret;
    uint64_t size = bio->bi_size;
    uint64_t off  = ((bio->bi_sector) << 9); 
    uint8_t  dir  = bio_data_dir(bio);

    struct HookRequest hreq;
    hreq.magic = MSG_MAGIC;
    /*READ:0 WRITE:1*/
    hreq.type   = dir ? IO_WRITE : IO_READ;
    hreq.handle = (uint64_t)bio;
    hreq.offset = off;
    hreq.len    = size;
    
    ret = tp_send(dev->network, (const char*)&hreq, sizeof(hreq));
    if(ret){
        LOG_ERR("send req failed ret:%d len:%lu", ret, sizeof(hreq));
        return ret;
    }
    
    /*send write io data*/
    if(dir == WRITE){
        int i;
        struct bio_vec* bvec;
        bio_for_each_segment(bvec, bio, i)
        {
            ret = net_send_bvec(dev, bvec);
            if(ret){
                LOG_ERR("send bvec failed ret:%d len:%d", ret, bvec->bv_len);
                return ret;
            }
            LOG_ERR("send bvec ok ret:%d len:%d", ret, bvec->bv_len);
        }
    }

    return 0;
}


static int net_recv_bvec(struct pbdev* dev, struct bio_vec* bvec)
{
    int ret;
    void* kaddr = kmap(bvec->bv_page);
    ret = tp_recv(dev->network, kaddr + bvec->bv_offset, bvec->bv_len);
    kunmap(bvec->bv_page);
    return ret;
}

static struct request* net_recv_req(struct pbdev* dev)
{
    int ret;
    struct HookReply reply;
    struct request* req;
    ret = tp_recv(dev->network, (char*)(&reply), sizeof(reply));
    if(ret){
        LOG_ERR("recv req head failed");
        return NULL;
    }
    LOG_ERR("recv req head ok");
    /*todo check*/
    if(reply.magic != MSG_MAGIC){
        LOG_ERR("recv req head error"); 
        return NULL;
    }

    req = (struct request*)reply.handle;
    
    if(rq_data_dir(req) == READ){
        struct req_iterator iter;
        struct bio_vec*     bvec;
        rq_for_each_segment(bvec, req, iter){
            ret = net_recv_bvec(dev, bvec);
            if(ret){
                LOG_ERR("recv bvec failed ret:%d len:%d", ret, bvec->bv_len);
            }
        }
    }
    return req;
}

static struct bio* net_recv_bio(struct pbdev* dev)
{
    int ret;
    struct HookReply reply;
    struct bio* bio;
    ret = tp_recv(dev->network, (char*)(&reply), sizeof(reply));
    if(ret){
        LOG_ERR("recv req head failed");
        return NULL;
    }
    /*todo check*/
    if(reply.magic != MSG_MAGIC){
        LOG_ERR("recv req head error"); 
        return NULL;
    }
    
    bio = (struct bio*)reply.handle;
    
    if(bio_data_dir(bio) == READ){
        int i;
        struct bio_vec* bvec;
        bio_for_each_segment(bvec, bio, i){
            ret = net_recv_bvec(dev, bvec);
            if(ret){
                LOG_ERR("recv bvec failed ret:%d len:%d", ret, bvec->bv_len);
            }
            LOG_ERR("recv bvec ok ret:%d len:%d", ret, bvec->bv_len);
        }
    }
    
    return bio;
}

static int send_work(void* data)
{
    struct pbdev* dev = (struct pbdev*)data;
#if 0
    struct request* req;
    while(!kthread_should_stop() || 
          !list_empty(&dev->send_queue) ||
          !bio_list_empty(&dev->send_bio_list)){
        
        wait_event_interruptible(dev->send_wq, 
                                 kthread_should_stop() || 
                                 !list_empty(&dev->send_queue) ||
                                 !bio_list_empty(&dev->send_bio_list));
        if(list_empty(&dev->send_queue)){
            continue; 
        }
        spin_lock_irq(&dev->blk_queue_lock);
        req = list_entry(dev->send_queue.next, struct request, queuelist);
        list_del_init(&req->queuelist);
        spin_unlock_irq(&dev->blk_queue_lock);
        LOG_ERR("send req:%d", req->cmd[0]);
        net_send_req(dev, req);
    }
#else
    while(!kthread_should_stop() || !bio_list_empty(&dev->send_bio_list))
    {
        wait_event_interruptible(dev->send_wq, kthread_should_stop() || 
                                 !bio_list_empty(&dev->send_bio_list));
        while(!bio_list_empty(&dev->send_bio_list))
        {
            struct bio* bio = bio_list_pop(&dev->send_bio_list);
            if(bio){
                net_send_bio(dev, bio); 
            }
        }
    }
#endif
    return 0;
}

static int recv_work(void* data)
{
    struct pbdev* dev = (struct pbdev*)data;
    struct bio* bio;
    while(!kthread_should_stop()){
        bio = net_recv_bio(dev);
        if(bio != NULL){
            bio_endio(bio, 0); 
        }
#if 0
        struct request* req;
        req = net_recv_req(dev);
        if(req != NULL){
            int error = 0;
            struct request_queue* q = req->q;
            unsigned long flags;
            LOG_INFO("recv req:%d", req->cmd[0]);
            /*ack request to block layer*/
            spin_lock_irqsave(q->queue_lock, flags);
            __blk_end_request_all(req, error);
            spin_unlock_irqrestore(q->queue_lock, flags);
        }
#endif
    }
    return 0;
}

int blk_dev_protect(const char* dev_path, const char* vol_name)
{
    int ret = 0;
    struct pbdev* dev;
    
    LOG_INFO("protect dev_path:%s", dev_path);
    dev = pbdev_mgr_get_by_path(&g_dev_mgr, dev_path);
    if(dev != NULL){
        LOG_ERR("dev:%s has protected", dev_path);
        goto err;
    }

    dev = kzalloc(sizeof(struct pbdev), GFP_KERNEL);
    if(dev == NULL){
        ret = -ENOMEM;
        LOG_ERR("allocte memory failed");
        goto err;
    }
    spin_lock_init(&(dev->lock));
    spin_lock_init(&(dev->blk_queue_lock));
    INIT_LIST_HEAD(&(dev->link));
    dev->blk_path = kstrdup(dev_path, GFP_KERNEL);
    dev->vol_name = kstrdup(vol_name, GFP_KERNEL);
    if(IS_ERR(dev->blk_path)){
        ret = -ENOMEM;
        LOG_ERR("allocte memory failed");
        goto err;
    }
    /*get block_device*/
    dev->blk_device = blkdev_get_by_path(dev->blk_path, FMODE_READ|FMODE_WRITE, NULL);
    if(IS_ERR(dev->blk_device)){
        ret = PTR_ERR(dev->blk_device);
        LOG_ERR("blkdev get by path failed:%d", ret);
        goto err;
    }

    dev->blk_queue = bdev_get_queue(dev->blk_device);
    if(IS_ERR(dev->blk_queue)){
        ret = PTR_ERR(dev->blk_device);
        LOG_ERR("blkdev get queue failed:%d", ret);
        goto err;
    }

    dev->blk_request_fn = dev->blk_queue->request_fn;
    dev->blk_bio_fn     = dev->blk_queue->make_request_fn;
    /*init transport*/
    dev->network = kzalloc(sizeof(struct transport), GFP_KERNEL);
    if(IS_ERR(dev->network)){
        ret = PTR_ERR(dev->blk_device);
        LOG_ERR("allocte memory failed");
        goto err;
    }
    spin_lock(&g_dev_mgr.dev_lock);
    ret = tp_create(dev->network, g_dev_mgr.sg_host, g_dev_mgr.sg_port);
    spin_unlock(&g_dev_mgr.dev_lock);
    if(ret){
        LOG_ERR("network create failed");
        goto err;
    }
    ret = tp_connect(dev->network);
    if(ret){
        LOG_ERR("network connect failed");
        goto err;
    }
    ret = net_send_vol_ctl_cmd(dev,true);
    if(ret) goto err;
        
    /*work thread*/
    INIT_LIST_HEAD(&dev->send_queue);
    INIT_LIST_HEAD(&dev->recv_queue);
    bio_list_init(&dev->send_bio_list);
    bio_list_init(&dev->recv_bio_list);
    init_waitqueue_head(&dev->send_wq);
    init_waitqueue_head(&dev->recv_wq);
    dev->send_thread = kthread_run(send_work, dev, "send_thread");
    dev->recv_thread = kthread_run(recv_work, dev, "recv_thread");
    if(IS_ERR(dev->send_thread) || IS_ERR(dev->recv_thread)){
        ret = PTR_ERR(dev->send_thread) | PTR_ERR(dev->recv_thread);
        LOG_ERR("create thread failed:%d", ret);
        goto err;
    }
    LOG_INFO("install hook,dev:%s",dev_path);
    /*install hook*/
    ret = install_hook(dev, hook_request_fn, hook_make_request_fn);
    if(ret){
        LOG_ERR("blkdev get queue failed:%d", ret);
        goto err;
    }
    LOG_INFO("install hook ok,dev:%s",dev_path);

    /*add mgr*/
    ret = pbdev_mgr_add(&g_dev_mgr, dev);
    return 0;
err:
    /*todo */
    if(dev){
        uninstall_hook(dev); 
        if(dev->send_thread){
            kthread_stop(dev->send_thread);
        }
        if(dev->recv_thread){
            kthread_stop(dev->recv_thread);   
        }
        if(dev->network){
            tp_close(dev->network);
            kfree(dev->network);
        }
        if(dev->blk_path){
            kfree(dev->blk_path);    
        }
        if(dev->vol_name){
            kfree(dev->vol_name);
        }
        kfree(dev);
    }
    return -1;
}

int blk_dev_unprotect(const char* dev_path)
{
    struct pbdev* dev;
    dev = pbdev_mgr_get_by_path(&g_dev_mgr, dev_path);
    if(dev == NULL){
        LOG_ERR("dev:%s no exist", dev_path);
        return 0;
    }
    uninstall_hook(dev); 
    LOG_INFO("uninstall hook,dev:%s", dev_path);
    
    net_send_vol_ctl_cmd(dev,false);
    if(dev->send_thread){
        kthread_stop(dev->send_thread);
    }
    LOG_INFO("stop send thread");
    if(dev->recv_thread){
        kthread_stop(dev->recv_thread);   
    }
    LOG_INFO("stop recv thread");
    if(dev->network){
        tp_close(dev->network);
        kfree(dev->network);
    }
    LOG_INFO("close network");
    if(dev->blk_path){
        kfree(dev->blk_path);    
    }
    if(dev->vol_name){
        kfree(dev->vol_name);
    }
    pbdev_mgr_del(&g_dev_mgr, dev_path);
    LOG_INFO("delete from mgr");
    kfree(dev);
    LOG_INFO("unprotect ok,dev:%s",dev_path);
    return 0;
}

int pbdev_mgr_init(struct pbdev_mgr* dev_mgr)
{
    dev_mgr->sg_host = NULL;
    spin_lock_init(&(dev_mgr->dev_lock));
    INIT_LIST_HEAD(&(dev_mgr->dev_list));
    return 0;
}

void pbdev_mgr_fini(struct pbdev_mgr* dev_mgr)
{
    struct pbdev* bdev;
    struct pbdev* tmp;
    list_for_each_entry_safe(bdev, tmp, &dev_mgr->dev_list, link)
    {
        blk_dev_unprotect(bdev->blk_path);
    }
    if(dev_mgr->sg_host)
    {
        kfree(dev_mgr->sg_host);
        dev_mgr->sg_host = NULL;
    }
}

int pbdev_mgr_add(struct pbdev_mgr* dev_mgr, struct pbdev* dev)
{
    spin_lock(&dev_mgr->dev_lock);
    list_add_tail(&dev->link, &dev_mgr->dev_list);
    spin_unlock(&dev_mgr->dev_lock);
    return 0;
}

int pbdev_mgr_del(struct pbdev_mgr* dev_mgr, const char* blk_path)
{
    struct pbdev* bdev;
    struct pbdev* tmp;
    spin_lock(&dev_mgr->dev_lock);
    list_for_each_entry_safe(bdev, tmp, &dev_mgr->dev_list, link){
        if(strcmp(bdev->blk_path, blk_path) == 0){
            list_del_init(&bdev->link); 
            spin_unlock(&dev_mgr->dev_lock);
            return 0;
        } 
    }
    spin_unlock(&dev_mgr->dev_lock);
    return -ENOENT;
}

struct pbdev* pbdev_mgr_get_by_path(struct pbdev_mgr* dev_mgr, const char* blk_path)
{
    struct pbdev* bdev;
    struct pbdev* tmp;
    spin_lock(&dev_mgr->dev_lock);
    list_for_each_entry_safe(bdev, tmp, &(dev_mgr->dev_list), link){
        if(strcmp(bdev->blk_path, blk_path) == 0){
            spin_unlock(&dev_mgr->dev_lock);
            return bdev;
        } 
    }
    spin_unlock(&dev_mgr->dev_lock);
    return NULL;
}

struct pbdev* pbdev_mgr_get_by_queue(struct pbdev_mgr* dev_mgr, struct request_queue* q)
{
    struct pbdev* bdev;
    struct pbdev* tmp;
    spin_lock(&dev_mgr->dev_lock);
    list_for_each_entry_safe(bdev, tmp, &dev_mgr->dev_list, link){
        if(bdev->blk_queue == q){
            spin_unlock(&dev_mgr->dev_lock);
            return bdev;
        } 
    }
    spin_unlock(&dev_mgr->dev_lock);
    return ERR_PTR(-ENOENT);
}
