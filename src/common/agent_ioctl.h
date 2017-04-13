#ifndef _AGENT_IOCTL_H_
#define _AGENT_IOCTL_H_

#include <linux/types.h>
#include <linux/ioctl.h>

struct agent_init
{
	pid_t pid;
	char* host;
	int   port;
};

struct agent_ioctl_add_dev
{
    char* vol_name;
	char* dev_path;
};

struct agent_ioctl_del_dev
{
	char* dev_path;
};


#define AGENT_CTL_DEVICE_PATH  ("/dev/sg_agent")

#define AGENT_IOCTL_BASE (0x4B)

#define AGENT_INIT _IOW(AGENT_IOCTL_BASE, 0, struct agent_init)

#define AGENT_ADD_DEVICE  _IOW(AGENT_IOCTL_BASE, 1, struct agent_ioctl_add_dev)

#define AGENT_DEL_DEVICE _IOW(AGENT_IOCTL_BASE, 2, struct agent_ioctl_del_dev)

#endif
