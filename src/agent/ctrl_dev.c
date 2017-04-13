#include <linux/module.h>
#include <linux/fs.h>
#include <linux/string.h>
#include <linux/err.h>
#include <linux/slab.h>
#include <linux/miscdevice.h>
#include <asm/uaccess.h>
#include "log.h"
#include "ctrl_dev.h"
#include "blk_dev.h"


static int parse_init_para(unsigned long arg,struct pbdev_mgr* dev_mgr)
{
    int ret;
    struct agent_init paras;
    ret = copy_from_user(&paras, (struct agent_init __user*)arg, \
                          sizeof(struct agent_init));
    if(ret){
        LOG_ERR("copy agent_init para failed:%d", ret);
        return -1;
    }
    spin_lock(&dev_mgr->dev_lock);
    if(dev_mgr->sg_host)
    {
        kfree(dev_mgr->sg_host);
        dev_mgr->sg_host = NULL;
    }
    dev_mgr->sg_host = strndup_user(paras.host, PAGE_SIZE);
    dev_mgr->sg_port = paras.port;
    dev_mgr->sg_pid = paras.pid;
    spin_unlock(&dev_mgr->dev_lock);

    LOG_INFO("sg_pid:%d,sg_host:%s,sg_port:%d", dev_mgr->sg_pid, \
        dev_mgr->sg_host,dev_mgr->sg_port);
    return 0;
}

static int parse_device_para(unsigned long arg, char** dev_path)
{
    int ret;
    char* blk_dev = NULL;
    struct agent_ioctl_add_dev paras;

    ret = copy_from_user(&paras, (struct agent_ioctl_add_dev __user*)arg, \
                          sizeof(struct agent_ioctl_add_dev));
    if(ret){
        LOG_ERR("copy agent_ioctl_add_dev para failed:%d", ret);
        goto err;
    }
    blk_dev = strndup_user(paras.dev, PAGE_SIZE);
    if(IS_ERR(blk_dev)){
        ret = PTR_ERR(blk_dev);
        LOG_ERR("strdup bdev failed:%d", ret);
        goto err;
    }

    *dev_path = blk_dev;
    return 0;
err:
    if(blk_dev) kfree(blk_dev);
    return -1;
}

static long ctrl_dev_ioctl(struct file* filp, unsigned int cmd, unsigned long arg)
{
    int ret = 0;
    char* dev_path = NULL;

    switch(cmd)
    {
        case AGENT_INIT:
            LOG_INFO("Agent ioctl init");
            ret = parse_init_para(arg, &g_dev_mgr);
            break;
        case AGENT_ADD_DEVICE:
            LOG_INFO("Agent ioctl add device");
            ret = parse_device_para(arg, &dev_path);
            LOG_INFO("dev_path:%s", dev_path);
            ret = blk_dev_protect(dev_path);
            break;
        case AGENT_DEL_DEVICE:
            LOG_INFO("Agent ioctl del device");
            ret = parse_device_para(arg, &dev_path);
            ret = blk_dev_unprotect(dev_path);
            break;
        default:
            LOG_INFO("Agent ioctl default");
            break;
    }

    if(dev_path) kfree(dev_path);
    return ret;
}

static struct file_operations ctrl_dev_fops = {
    .owner          = THIS_MODULE,
    .open           = nonseekable_open,
    .unlocked_ioctl = ctrl_dev_ioctl,
    .compat_ioctl   = ctrl_dev_ioctl,
    .llseek         = no_llseek,
};

static struct miscdevice ctrl_dev = {
    .minor = MISC_DYNAMIC_MINOR,
    .name  = AGENT_CTL_DEVICE_PATH,
    .fops  = &ctrl_dev_fops,
};

int ctrl_dev_init(void)
{
    int ret;
    ret = misc_register(&ctrl_dev);
    if(ret){
        LOG_ERR("misc register failed:%d", ret);
        return ret;
    }
    LOG_INFO("misc register agent ctrl ok");
    return ret;
}

void ctrl_dev_exit(void)
{
    misc_deregister(&ctrl_dev);
    LOG_INFO("misc unregister agent ctrl ok");
}
