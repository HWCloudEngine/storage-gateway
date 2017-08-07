#include <linux/kthread.h>
#include <linux/string.h>
#include <linux/blkdev.h>
#include <linux/blk_types.h>
#include <linux/sched.h>
#include <linux/delay.h>
#include <linux/pid.h>
#include <linux/version.h>
#include <linux/vmalloc.h>
#include <linux/jiffies.h>
#include <asm/barrier.h>
#include <scsi/scsi_host.h>
#include <scsi/scsi_cmnd.h>
#include <scsi/scsi_device.h>
#include "log.h"
#include "message.h"
#include "blk_dev.h"
#include <linux/highmem.h>

struct pbdev_mgr g_dev_mgr;

static int pbdev_mgr_add(struct pbdev_mgr* dev_mgr, struct pbdev* dev)
{
    if (dev_mgr && dev) {
        spin_lock_irq(&dev_mgr->dev_lock);
        list_add_tail(&dev->link, &dev_mgr->dev_list);
        spin_unlock_irq(&dev_mgr->dev_lock);
    }
    return 0;
}

static int pbdev_mgr_del(struct pbdev_mgr* dev_mgr, const char* blk_path)
{
    if (dev_mgr && blk_path) {
        struct pbdev* bdev = NULL;
        struct pbdev* tmp = NULL;
        spin_lock_irq(&dev_mgr->dev_lock);
        if (list_empty(&(dev_mgr->dev_list))) {
            spin_unlock_irq(&dev_mgr->dev_lock);
            return 0;
        }
        list_for_each_entry_safe(bdev, tmp, &dev_mgr->dev_list, link) {
            if(bdev && bdev->blk_path && strcmp(bdev->blk_path, blk_path) == 0){
                list_del_init(&bdev->link); 
                spin_unlock_irq(&dev_mgr->dev_lock);
                return 0;
            }
        }
        spin_unlock_irq(&dev_mgr->dev_lock);
    }
    return -ENOENT;
}

static struct pbdev* pbdev_mgr_get_by_path(struct pbdev_mgr* dev_mgr, const char* blk_path)
{
    if (dev_mgr && blk_path) {
        struct pbdev* bdev = NULL;
        struct pbdev* tmp = NULL;
        spin_lock_irq(&dev_mgr->dev_lock);
        if (list_empty(&(dev_mgr->dev_list))) {
            spin_unlock_irq(&dev_mgr->dev_lock);
            LOG_ERR("get by path failed dev_list is empty");
            return NULL;
        }
        list_for_each_entry_safe(bdev, tmp, &(dev_mgr->dev_list), link) {
            if(bdev && bdev->blk_path && strcmp(bdev->blk_path, blk_path) == 0){
                spin_unlock_irq(&dev_mgr->dev_lock);
                return bdev;
            } 
        }
        spin_unlock_irq(&dev_mgr->dev_lock);
    }
    return NULL;
}

static struct pbdev* pbdev_mgr_get_by_queue(struct pbdev_mgr* dev_mgr, struct request_queue* q)
{
    if (dev_mgr && q) {
        struct pbdev* bdev = NULL;
        struct pbdev* tmp = NULL;
        spin_lock_irq(&dev_mgr->dev_lock);
        if (list_empty(&(dev_mgr->dev_list))) {
            spin_unlock_irq(&dev_mgr->dev_lock);
            return NULL;
        }
        list_for_each_entry_safe(bdev, tmp, &dev_mgr->dev_list, link) {
            if(bdev && bdev->blk_queue && bdev->blk_queue == q){
                spin_unlock_irq(&dev_mgr->dev_lock);
                return bdev;
            }
        }
        spin_unlock_irq(&dev_mgr->dev_lock);
    }
    return ERR_PTR(-ENOENT);
}

static int cbt_alloc(struct pbdev* dev)
{
    if(dev && dev->blk_device){
        struct gendisk* bd_disk = dev->blk_device->bd_disk;
        if (bd_disk) {
            /*512*/
            size_t cbt_bitmap_bits = 0;
            dev->granularity_shit  = 9;
            dev->granularity = (2 << dev->granularity_shit);
            cbt_bitmap_bits = (bd_disk->part0.nr_sects << 9) >> dev->granularity_shit;
            dev->cbt_bitmap_size = BITS_TO_LONGS(cbt_bitmap_bits) * sizeof(unsigned long);
            dev->cbt_bitmap = vmalloc(dev->cbt_bitmap_size);
            if (!dev->cbt_bitmap) {
                LOG_ERR("allocate cbt bitmap faild"); 
                return -ENOMEM;
            }
            //bitmap_zero(dev->cbt_bitmap, cbt_bitmap_bits);
            LOG_INFO("%s: start:%lu nr:%lu", bd_disk->disk_name,
                    bd_disk->part0.start_sect, bd_disk->part0.nr_sects);
        }
    }
    return 0;
}

static void cbt_free(struct pbdev* dev)
{
    if(dev && dev->cbt_bitmap){
        vfree(dev->cbt_bitmap);
    }
}

static void cbt_set(struct pbdev* dev, sector_t start, sector_t nr_sects)
{
    off_t start_pos = start << 9;
    off_t end_pos = (start+nr_sects) << 9;
    while (start_pos < end_pos) {
        set_bit((start_pos>>dev->granularity_shit), dev->cbt_bitmap);
        start_pos += dev->granularity;
    }
}

static void cbt_clear(struct pbdev* dev, sector_t start, sector_t nr_sects)
{
    off_t start_pos = start << 9;
    off_t end_pos = (start+nr_sects) << 9; 
    while (start_pos < end_pos) {
        clear_bit((start_pos>>dev->granularity_shit), dev->cbt_bitmap);
        start_pos += dev->granularity;
    }
}

static bool cbt_check(struct pbdev* dev, sector_t start, sector_t nr_sects)
{
    off_t start_pos = start << 9;
    off_t end_pos = (start+nr_sects) << 9; 
    while (start_pos < end_pos) {
        if(!test_bit((start_pos >> dev->granularity_shit), (dev->cbt_bitmap)))
            return false;
        start_pos += dev->granularity;
    }
    return true;
}

#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
void tracer_make_request_fn(struct request_queue* q, struct bio* bio)
#else
blk_qc_t tracer_make_request_fn(struct request_queue* q, struct bio* bio)
#endif
{
    int pass = 0;
    pid_t cur_tgid = current->tgid;
    if(!q || !bio){
        LOG_ERR("queue or bio is null");
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
        return;
#else
        return BLK_QC_T_NONE;
#endif
    }
    LOG_INFO("backing device info:%s ", q->backing_dev_info.name);
    struct pbdev* dev = pbdev_mgr_get_by_queue(&g_dev_mgr, q);
    if(dev == NULL){
        LOG_ERR("get dev by queue failed");
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
        return;
#else
        return BLK_QC_T_NONE;
#endif
    }
    
    /*network exception first exhaust bio then passthrough*/
    if(atomic_read(&dev->network->isok) == 0){
        while(!bio_list_empty(&dev->send_bio_list)){
            msleep(10);     
        }
        dev->blk_bio_fn(q, bio);
        LOG_INFO("network exception bio passthrough");
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
        return;
#else
        return BLK_QC_T_NONE;
#endif
    }

    /* sg client bio */
    spin_lock_irq(&g_dev_mgr.dev_lock);
    pass = ((cur_tgid == g_dev_mgr.sg_pid) ? 1 : 0);
    spin_unlock_irq(&g_dev_mgr.dev_lock);
    if(pass){
        if(bio_data_dir(bio) == WRITE){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
            cbt_clear(dev, bio->bi_sector, (bio->bi_size >> 9)); 
#else
            cbt_clear(dev, bio->bi_iter.bi_sector, (bio->bi_iter.bi_size >> 9)); 
#endif
        }
        dev->blk_bio_fn(q,bio);
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
        return;
#else
        return BLK_QC_T_NONE;
#endif
    }
    if(bio_data_dir(bio) == WRITE){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
         cbt_set(dev, bio->bi_sector, (bio->bi_size >> 9)); 
#else
         cbt_set(dev, bio->bi_iter.bi_sector, (bio->bi_iter.bi_size >> 9)); 
#endif
    }
    if(bio_data_dir(bio) == READ){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
        int ret = cbt_check(dev, bio->bi_sector,(bio->bi_size >> 9));
#else
        int ret = cbt_check(dev, bio->bi_iter.bi_sector,(bio->bi_iter.bi_size >> 9));
#endif
        if(!ret){
            dev->blk_bio_fn(q,bio);
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
            return;
#else
            return BLK_QC_T_NONE;
#endif
        }
    }
    spin_lock_irq(&dev->lock);
    bio_list_add(&dev->send_bio_list, bio);
    spin_unlock_irq(&dev->lock);
    /*wakup network send thread*/
    wake_up(&dev->send_wq);
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
    return;
#else
    return BLK_QC_T_NONE;
#endif
}

static int install_tracer(struct pbdev* dev, make_request_fn* new_make_request_fn)
{
    int ret = 0;
    if(dev && dev->blk_device){
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
        if(dev->blk_device->bd_disk && dev->blk_device->bd_disk->queue){
            dev->blk_device->bd_disk->queue->make_request_fn = new_make_request_fn;
        }
        smp_wmb(); 
        if(sb){
            LOG_INFO("thrawing block device");
            ret = thaw_bdev(dev->blk_device, sb);
            if(ret){
                LOG_ERR("thaw bdev failed:%d", ret); 
                return ret;
            }
            LOG_INFO("thrawing block device ok");
        }
    }
    return ret;
}

static int uninstall_tracer(struct pbdev* dev)
{
    int ret = 0;
    if(dev && dev->blk_device){
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
            LOG_INFO("freezing block device ok");
        }
        smp_wmb(); 
        if(dev->blk_device->bd_disk && dev->blk_device->bd_disk->queue){
            dev->blk_device->bd_disk->queue->make_request_fn = dev->blk_bio_fn;
        }
        smp_wmb(); 
        if(sb){
            LOG_INFO("thrawing block device");
            ret = thaw_bdev(dev->blk_device, sb);
            if(ret){
                LOG_ERR("thaw bdev failed:%d", ret); 
            }
            LOG_INFO("thrawing block device ok");
        }
    }
    return ret;
}

void cmd_bio_callback(struct bio* bio)
{
    if(bio){
        struct pbdev* dev = (struct pbdev*)bio->bi_private;
        LOG_INFO("cmd bio callback");
        complete(&dev->cmd_sync_event);
        LOG_INFO("cmd bio callback ok");
    }
}

static struct bio* alloc_cmd_bio(struct pbdev* dev, int cmd_type) 
{
    struct bio* bio = NULL;
    bio = bio_alloc(GFP_NOIO, 0);
    if(!bio){
        LOG_ERR("alloc bio failed");
        return NULL;
    }
    bio->bi_flags = cmd_type;
    bio->bi_end_io = cmd_bio_callback;
    bio->bi_private = dev;
    return bio;
}

static void free_cmd_bio(struct bio* bio)
{
    if(bio){
        bio_put(bio);
    }
}

static void submit_cmd_bio(struct pbdev* dev, struct bio* bio)
{
    spin_lock_irq(&dev->lock);
    bio_list_add(&dev->send_bio_list, bio);
    spin_unlock_irq(&dev->lock);
    /*wakup network send thread*/
    wake_up(&dev->send_wq);
}

static int net_send_cmd(struct pbdev* dev, struct bio* bio)
{
    int ret = 0;
    io_request_t* req = NULL;
    int req_len = sizeof(io_request_t);
    if(bio->bi_flags == ADD_VOLUME){
        req_len += sizeof(add_vol_req_t);
    }
    if(bio->bi_flags == DEL_VOLUME) {
        req_len += sizeof(del_vol_req_t);
    }
    req = kzalloc(req_len, GFP_KERNEL);
    if(req == NULL){
        LOG_ERR("allocte memory failed");
        ret = -ENOMEM;
        goto out;
    }
    req->magic = MSG_MAGIC;
    req->type = bio->bi_flags;
    req->seq = dev->seq_id++;
    req->handle = (uint64_t)bio;
    req->offset = 0;
    if(bio->bi_flags == ADD_VOLUME){
        add_vol_req_t* add_vol = (add_vol_req_t*)req->data;
        strcpy(add_vol->vol_name, dev->vol_name);
        strcpy(add_vol->dev_path, dev->blk_path);
        req->len = sizeof(add_vol_req_t);
    }
    if(bio->bi_flags == DEL_VOLUME){
        del_vol_req_t* del_vol = (del_vol_req_t*)req->data;
        strcpy(del_vol->vol_name, dev->vol_name);
        req->len = sizeof(del_vol_req_t);
    }
    ret = tp_send(dev->network, (char*)req, req_len);
    if(ret != 0){
        LOG_ERR("vol cmd send err ret:%d size:%d ", ret, req_len);
        goto out;;
    }
out:
    if(req){
        kfree(req);
    }
    return ret;;
}

static int net_send_bvec(struct pbdev* dev, struct bio_vec* bvec)
{
    int ret = 0;
    void* kaddr = kmap(bvec->bv_page);
    ret = tp_send(dev->network, (const char*)kaddr + bvec->bv_offset, bvec->bv_len);
    kunmap(kaddr);
    return ret;
}

static int net_send_bio(struct pbdev* dev, struct bio* bio)
{
    int ret = 0;
    #if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
    uint64_t size = bio->bi_size;
    uint64_t off = ((bio->bi_sector) << 9); 
    #else
    uint64_t size = bio->bi_iter.bi_size;
    uint64_t off = ((bio->bi_iter.bi_sector) << 9); 
    #endif
    uint8_t dir = bio_data_dir(bio);
    io_request_t hreq = {0};
    hreq.magic = MSG_MAGIC;
    /*READ:0 WRITE:1*/
    hreq.type = dir ? IO_WRITE : IO_READ;
    hreq.seq = dev->seq_id++;
    hreq.handle = (uint64_t)bio;
    hreq.offset = off;
    hreq.len = size;
    ret = tp_send(dev->network, (const char*)&hreq, sizeof(hreq));
    if(ret){
        LOG_ERR("send io request failed ret:%d len:%lu", ret, sizeof(hreq));
        return ret;
    }
    // LOG_INFO("send bio dir:%d off:%llu len:%llu hdl:%llu", dir, off, size, hreq.handle); 
    /*send write io data*/
    if(dir == WRITE){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
        int i;
        struct bio_vec* bvec;
        bio_for_each_segment(bvec, bio, i) {
            ret = net_send_bvec(dev, bvec);
            if(ret){
                LOG_ERR("send bvec failed ret:%d", ret);
                return ret;
            }
        }
#else
        {
            struct bio_vec bv = {NULL, 0, 0};
            struct bvec_iter iter = {0};
            bio_for_each_segment(bv, bio, iter){
                ret = net_send_bvec(dev, &bv);
                if(ret){
                    LOG_ERR("send bvec failed ret:%d", ret);
                    return ret;
                }
                // LOG_INFO("send bvec off:%d len:%d", bv.bv_offset, bv.bv_len);
            }
        }
#endif
    }

    return 0;
}

static int net_recv_bvec(struct pbdev* dev, struct bio_vec* bvec)
{
    int ret = 0;
    void* kaddr = kmap(bvec->bv_page);
    ret = tp_recv(dev->network, (char*)kaddr + bvec->bv_offset, bvec->bv_len);
    kunmap(bvec->bv_page);
    return ret;
}

static struct bio* net_recv_bio(struct pbdev* dev)
{
    int ret = 0;
    io_reply_t reply = {0};
    struct bio* bio = NULL;
    ret = tp_recv(dev->network, (char*)(&reply), sizeof(reply));
    if(ret){
        LOG_ERR("recv req head failed");
        return NULL;
    }
    if(reply.magic != MSG_MAGIC){
        LOG_ERR("recv req head error"); 
        return NULL;
    }
    // LOG_INFO("recv bio hdl:%llu", reply.handle); 
    bio = (struct bio*)reply.handle;
    if(bio == NULL){
        return NULL;
    }
    if(bio_data_dir(bio) == READ){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
        int i;
        struct bio_vec* bvec;
        bio_for_each_segment(bvec, bio, i){
            ret = net_recv_bvec(dev, bvec);
            if(ret){
                LOG_ERR("recv bvec failed ret:%d", ret);
            }
        }
#else
        {
            struct bio_vec bvec = {0};
            struct bvec_iter bio_iter = {0};
            bio_for_each_segment(bvec, bio, bio_iter){
                ret = net_recv_bvec(dev, &bvec);
                if(ret){
                    LOG_ERR("recv bvec failed ret:%d", ret);
                }
            }
            // LOG_INFO("recv bvec off:%llu len:%llu", bvec.bv_offset, bvec.bv_len);
        }
#endif
    }
    return bio;
}

static int send_work(void* data)
{
    int ret = 0;
    struct pbdev* dev = (struct pbdev*)data;
    struct bio* bio = NULL;
    unsigned long flags;
    spin_lock_irqsave(&dev->send_thread_lock, flags);
    dev->send_thread = current;
    spin_unlock_irqrestore(&dev->send_thread_lock, flags);

    while(!kthread_should_stop() || !bio_list_empty(&dev->send_bio_list))
    {
        wait_event_interruptible(dev->send_wq, kthread_should_stop() || 
                                 !bio_list_empty(&dev->send_bio_list));
        if(signal_pending(current)){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
            siginfo_t info;
            ret = dequeue_signal_lock(current, &current->blocked, &info);
#else
            ret = kernel_dequeue_signal(NULL);
#endif
            LOG_INFO("0 got signal %d now", ret);
            break;
        }
        while(!bio_list_empty(&dev->send_bio_list))
        {
            spin_lock_irq(&dev->lock);
            bio = bio_list_pop(&(dev->send_bio_list));
            spin_unlock_irq(&dev->lock);
            if(!bio){
                LOG_ERR("bio pop failed bio null");
                break;
            }
            if(bio->bi_flags == ADD_VOLUME){
                LOG_INFO("send add vol cmd");
                ret = net_send_cmd(dev, bio);
                if(ret) {
                    LOG_INFO("send add vol cmd fail");
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
                    bio_endio(bio, 0); 
#else
                    bio->bi_error = 0;
                    bio_endio(bio); 
#endif
                }
            } else if(bio->bi_flags == DEL_VOLUME) {
                LOG_INFO("send del vol cmd");
                ret = net_send_cmd(dev, bio);
                if(ret) {
                    LOG_INFO("send del vol cmd fail");
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
                    bio_endio(bio, 0); 
#else
                    bio->bi_error = 0;
                    bio_endio(bio); 
#endif
                }
            } else {
                ret = net_send_bio(dev, bio); 
                if(ret) {
                    LOG_ERR("send bio failure passthrough bio");
                    dev->blk_bio_fn(dev->blk_queue, bio);
                    LOG_ERR("send bio failure passthrough bio ok");
                }
            }
        }
    }

    spin_lock_irqsave(&dev->send_thread_lock, flags);
    dev->send_thread = NULL;
    spin_unlock_irqrestore(&dev->send_thread_lock, flags);
    if(signal_pending(current)){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
        siginfo_t info;
        ret = dequeue_signal_lock(current, &current->blocked, &info);
#else
        ret = kernel_dequeue_signal(NULL);
#endif
        LOG_INFO("1 got signal %d now", ret);
    }
    return 0;
}

static int recv_work(void* data)
{
    struct pbdev* dev = (struct pbdev*)data;
    struct bio* bio = NULL;
    unsigned long flags;
    spin_lock_irqsave(&dev->recv_thread_lock, flags);
    dev->recv_thread = current;
    spin_unlock_irqrestore(&dev->recv_thread_lock, flags);

    while(!kthread_should_stop()){
        bio = net_recv_bio(dev);
        if(bio == NULL){
            LOG_ERR("net recv bio error");
            break;
        }
#if (LINUX_VERSION_CODE < KERNEL_VERSION(3,14,0))
        bio_endio(bio, 0); 
#else
        bio->bi_error = 0;
        bio_endio(bio); 
#endif
        if(bio->bi_flags == DEL_VOLUME){
            break;
        }
    }

    spin_lock_irqsave(&dev->recv_thread_lock, flags);
    dev->recv_thread = NULL;
    spin_unlock_irqrestore(&dev->recv_thread_lock, flags);
    if(signal_pending(current)){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
        siginfo_t info;
        int ret = dequeue_signal_lock(current, &current->blocked, &info);
#else
        int ret = kernel_dequeue_signal(NULL);
#endif
        LOG_INFO("2 got signal %d now", ret);
    }
    return 0;
}

static struct pbdev* pbdev_alloc(void)
{
    struct pbdev* dev = NULL;
    dev = kzalloc(sizeof(struct pbdev), GFP_KERNEL);
    return dev;
}

static void pbdev_free(struct pbdev* dev)
{
    if(dev){
        kfree(dev); 
    }
}

static int pbdev_deinit(struct pbdev* dev)
{
    if(dev){
        unsigned long flags;
        uninstall_tracer(dev); 
        spin_lock_irqsave(&dev->send_thread_lock, flags);
        if(dev->send_thread){
            force_sig(SIGKILL, dev->send_thread);
            //kthread_stop(dev->send_thread);
            LOG_INFO("send thread stop ok");
        }
        spin_unlock_irqrestore(&dev->send_thread_lock, flags);

        spin_lock_irqsave(&dev->recv_thread_lock, flags);
        if(dev->recv_thread){
            force_sig(SIGKILL, dev->recv_thread);
            //kthread_stop(dev->recv_thread);   
            LOG_INFO("recv thread stop ok");
        }
        spin_unlock_irqrestore(&dev->recv_thread_lock, flags);
        if(dev->network){
            tp_close(dev->network);
            kfree(dev->network);
        }
        if(dev->cbt_bitmap){
            cbt_free(dev);
        }
        if(dev->blk_path){
            kfree(dev->blk_path);    
        }
        if(dev->vol_name){
            kfree(dev->vol_name);
        }
        if(dev->blk_device){
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
            blkdev_put(dev->blk_device, FMODE_READ|FMODE_WRITE);
#else
            /*linux kernel 4.0 may occur deadlock between sgclient and system-udevd*/
            //blkdev_put(dev->blk_device, FMODE_READ|FMODE_WRITE);
#endif
        }
        return 0;
    }
    return -1;
}

static int pbdev_init(struct pbdev* dev, const char* dev_path, const char* vol_name)
{
    int ret = 0;
    dev->seq_id = 0;
    spin_lock_init(&(dev->lock));
    spin_lock_init(&(dev->blk_queue_lock));
    INIT_LIST_HEAD(&(dev->link));
    dev->blk_path = kstrdup(dev_path, GFP_KERNEL);
    dev->vol_name = kstrdup(vol_name, GFP_KERNEL);
    if(IS_ERR(dev->blk_path) || IS_ERR(dev->vol_name)){
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
    ret = cbt_alloc(dev);
    if(ret){
        LOG_ERR("cbt alloc failed");
        goto err;
    }
    dev->blk_queue = bdev_get_queue(dev->blk_device);
    if(IS_ERR(dev->blk_queue)){
        ret = PTR_ERR(dev->blk_device);
        LOG_ERR("blkdev get queue failed:%d", ret);
        goto err;
    }
    dev->blk_request_fn = dev->blk_queue->request_fn;
    dev->blk_bio_fn = dev->blk_queue->make_request_fn;
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
    init_completion(&dev->cmd_sync_event);
    /*work thread*/
    bio_list_init(&dev->send_bio_list);
    bio_list_init(&dev->recv_bio_list);
    init_waitqueue_head(&dev->send_wq);
    init_waitqueue_head(&dev->recv_wq);
    spin_lock_init(&dev->send_thread_lock);
    spin_lock_init(&dev->recv_thread_lock);
    dev->send_thread = kthread_run(send_work, dev, "send_thread");
    dev->recv_thread = kthread_run(recv_work, dev, "recv_thread");
    if(IS_ERR(dev->send_thread) || IS_ERR(dev->recv_thread)){
        ret = PTR_ERR(dev->send_thread) | PTR_ERR(dev->recv_thread);
        LOG_ERR("create thread failed:%d", ret);
        goto err;
    }
    LOG_INFO("install dev:%s tracer",dev_path);
    /*install tracer*/
    ret = install_tracer(dev, &tracer_make_request_fn);
    if(ret){
        LOG_ERR("install dev:%s tracer failed:%d", dev_path, ret);
        goto err;
    }
    LOG_INFO("install dev:%s tracer ok",dev_path);
    LOG_INFO("bdev init dev:%s ok", dev_path);
    return ret;
err:
    LOG_INFO("bdev init dev:%s failed", dev_path);
    if(dev){
        pbdev_deinit(dev);
    }
    return ret;
}


static bool block_device_exist(const char* dev_path)
{
    struct block_device* bdev = NULL;
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,4,0))
    bdev = lookup_bdev(dev_path);
#else
    bdev = lookup_bdev(dev_path, 0);
#endif
    if(IS_ERR(bdev)){
        LOG_ERR("%s not exist", dev_path);
        return false;
    }
    return true;
}

int pbdev_mgr_init(struct pbdev_mgr* dev_mgr)
{
    if (dev_mgr) {
        dev_mgr->sg_host = NULL;
        spin_lock_init(&(dev_mgr->dev_lock));
        INIT_LIST_HEAD(&(dev_mgr->dev_list));
    }
    return 0;
}

void pbdev_mgr_fini(struct pbdev_mgr* dev_mgr)
{
    if (dev_mgr) {
        struct pbdev* bdev = NULL;
        struct pbdev* tmp = NULL;
        list_for_each_entry_safe(bdev, tmp, &dev_mgr->dev_list, link) {
            if (bdev && bdev->blk_path) {
                blk_dev_unprotect(bdev->blk_path);
            }
        }
        if (dev_mgr->sg_host) {
            kfree(dev_mgr->sg_host);
            dev_mgr->sg_host = NULL;
        }
    }
}

int blk_dev_protect(const char* dev_path, const char* vol_name)
{
    int ret = 0;
    struct pbdev* dev = NULL;
    struct bio* add_vol_bio = NULL;
    LOG_INFO("protect dev_path:%s vol_name:%s", dev_path, vol_name);
    if(!block_device_exist(dev_path)){
        LOG_ERR("dev_path:%s not exist", dev_path);
        ret = -ENOENT;
        goto err;
    }
    dev = pbdev_mgr_get_by_path(&g_dev_mgr, dev_path);
    if(dev != NULL){
        LOG_ERR("dev:%s has protected", dev_path);
        ret = -EEXIST;
        goto err;
    }
    dev = pbdev_alloc();
    if(dev == NULL){
        ret = -ENOMEM;
        LOG_ERR("allocte memory failed");
        goto err;
    }
    ret = pbdev_init(dev, dev_path, vol_name);
    if(ret){
        LOG_ERR("pbdev init failed");
        goto err;
    }
    add_vol_bio = alloc_cmd_bio(dev, ADD_VOLUME);
    if(!add_vol_bio){
        LOG_ERR("alloc ctrl bio failed");
        ret = -ENOMEM;
        goto err;
    }
    LOG_INFO("wait add vol bio");
    submit_cmd_bio(dev, add_vol_bio);
    wait_for_completion(&dev->cmd_sync_event);
    //if(!wait_for_completion_timeout(&dev->cmd_sync_event, msecs_to_jiffies(200))){
    //    LOG_ERR("wait add vol bio timeout");
    //} else {
    //    LOG_INFO("wait add vol bio ok");
    //}
    free_cmd_bio(add_vol_bio);
    /*add mgr*/
    ret = pbdev_mgr_add(&g_dev_mgr, dev);
    LOG_INFO("protect dev_path:%s ok", dev_path);
    return 0;
err:
    if(dev){
        pbdev_deinit(dev);
        pbdev_free(dev);
    }
    LOG_ERR("protect dev_path:%s failed", dev_path);
    return -1;
}

int blk_dev_unprotect(const char* dev_path)
{
    struct pbdev* dev = NULL;
    struct bio* del_vol_bio = NULL;
    if (dev_path == NULL) {
        return 0; 
    }
    LOG_INFO("unprotect dev_path:%s", dev_path);
    if(!block_device_exist(dev_path)) {
        LOG_ERR("unprotect dev_path:%s no exist", dev_path);
        return 0;
    }
    dev = pbdev_mgr_get_by_path(&g_dev_mgr, dev_path);
    if(dev == NULL){
        LOG_ERR("unprotect dev_path:%s no exist", dev_path);
        return 0;
    }
    del_vol_bio = alloc_cmd_bio(dev, DEL_VOLUME);
    if(!del_vol_bio){
        LOG_ERR("alloc del bio failed");
        return 0;
    }
    LOG_INFO("wait del vol bio");
    submit_cmd_bio(dev, del_vol_bio);
    if(!wait_for_completion_timeout(&dev->cmd_sync_event, msecs_to_jiffies(200))){
        LOG_ERR("wait del vol bio timeout");
    } else {
        LOG_INFO("wait del vol bio ok");
    }
    free_cmd_bio(del_vol_bio);
    pbdev_mgr_del(&g_dev_mgr, dev_path);
    if(dev){
        pbdev_deinit(dev); 
        pbdev_free(dev);
    }
    LOG_INFO("unprotect dev_path:%s ok",dev_path);
    return 0;
}
