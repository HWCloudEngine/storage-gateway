#ifndef CTRL_DEV_H
#define CTRL_DEV_H
#include <linux/ioctl.h>
#include <linux/limits.h>

#define AGENT_IOCTL_MAGIC 0xAA

struct enable_protect_para
{
    char*    bdev;
    char*    host;
};

struct disable_protect_para
{
    char* bdev;
};

#define IOCTL_ENABLE_PROTECT  _IOW(AGENT_IOCTL_MAGIC, 1, struct enable_protect_para)
#define IOCTL_DISABLE_PROTECT _IOW(AGENT_IOCTL_MAGIC, 2, struct disable_protect_para)

int ctrl_dev_init(void);
void ctrl_dev_exit(void);

#endif
