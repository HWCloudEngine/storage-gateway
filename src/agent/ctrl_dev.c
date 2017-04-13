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

#define CTRL_DEV_NAME "agent_ctrl"

static int parse_enable_para(unsigned long arg, char** dev_path, char** sg_host)
{
    int ret;
    char* blk_dev = NULL;
    char* host = NULL;
    struct enable_protect_para paras;

    ret = copy_from_user(&paras, (struct enable_protect_para __user*)arg, \
                          sizeof(struct enable_protect_para));
    if(ret){
        LOG_ERR("copy enable para failed:%d", ret);
        goto err;
    }
    blk_dev = strndup_user(paras.bdev, PAGE_SIZE);
    if(IS_ERR(blk_dev)){
        ret = PTR_ERR(blk_dev);
        LOG_ERR("strdup bdev failed:%d", ret);
        goto err;
    }
    host = strndup_user(paras.host, PAGE_SIZE);
    if(IS_ERR(host)){
        ret = PTR_ERR(host);
        LOG_ERR("strdup host failed:%d", ret);
        goto err;
    }
    LOG_INFO(" blk_dev:%s host:%s", blk_dev, host);

    *dev_path = blk_dev;
    *sg_host  = host;
    return 0;
err:
    if(blk_dev) kfree(blk_dev);
    if(host) kfree(host);
    return ret;
}

static int parse_disable_para(unsigned long arg, char** dev_path)
{
    int ret;
    char* blk_dev = NULL;
    struct disable_protect_para paras;

    ret = copy_from_user(&paras, (struct disable_protect_para __user*)arg, \
                          sizeof(struct disable_protect_para));
    if(ret){
        LOG_ERR("copy disable para failed:%d", ret);
        goto err;
    }

    blk_dev = strndup_user(paras.bdev, PAGE_SIZE);
    if(IS_ERR(blk_dev)){
        ret = PTR_ERR(blk_dev);
        LOG_ERR("strdup bdev failed:%d", ret);
        goto err;
    }
    *dev_path = blk_dev;
    return 0;
err:
    if(blk_dev) kfree(blk_dev);
    return ret;
}

static long ctrl_dev_ioctl(struct file* filp, unsigned int cmd, unsigned long arg)
{
    int ret;
    char* dev_path = NULL;
    char* sg_host = NULL;

    LOG_INFO("dev ioctl cmd: %d pro:%lu, unpro:%lu ", cmd, IOCTL_ENABLE_PROTECT, \
            IOCTL_DISABLE_PROTECT);

    switch(cmd)
    {
        case IOCTL_ENABLE_PROTECT:
            LOG_INFO("xxxx 1");
            ret = parse_enable_para(arg, &dev_path, &sg_host);
            LOG_INFO("dev_path:%s sg_host:%s", dev_path, sg_host);
            ret = blk_dev_protect(dev_path, sg_host);
            break;
        case IOCTL_DISABLE_PROTECT:
            LOG_INFO("xxxx 2");
            ret = parse_disable_para(arg, &dev_path);
            ret = blk_dev_unprotect(dev_path);
            break;
        default:
            LOG_INFO("xxxx 3");
            break;
    }

    if(dev_path) kfree(dev_path);
    if(sg_host) kfree(sg_host);
    return 0;
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
    .name  = CTRL_DEV_NAME,
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
    LOG_INFO("misc register ok");
    return ret;
}

void ctrl_dev_exit(void)
{
    misc_deregister(&ctrl_dev);
    LOG_INFO("misc unregister ok");
}
