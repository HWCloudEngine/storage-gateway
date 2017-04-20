#ifndef _AGENT_IOCTL_H_
#define _AGENT_IOCTL_H_

#include <linux/types.h>
#include <linux/ioctl.h>

struct agent_init
{
<<<<<<< 340d3df79e730fee0d60652dc716f16ab528f579
	pid_t pid;
	char* host;
	int   port;
=======
    pid_t pid;
    char* host;
    int   port;
>>>>>>> Agent mode control module
};

struct agent_ioctl_add_dev
{
    char* vol_name;
<<<<<<< 340d3df79e730fee0d60652dc716f16ab528f579
	char* dev_path;
=======
    char* dev_path;
>>>>>>> Agent mode control module
};

struct agent_ioctl_del_dev
{
<<<<<<< 340d3df79e730fee0d60652dc716f16ab528f579
	char* dev_path;
=======
    char* dev_path;
>>>>>>> Agent mode control module
};


#define AGENT_CTL_DEVICE_PATH  ("/dev/sg_agent")

#define AGENT_IOCTL_BASE (0x4B)

#define AGENT_INIT _IOW(AGENT_IOCTL_BASE, 0, struct agent_init)

#define AGENT_ADD_DEVICE  _IOW(AGENT_IOCTL_BASE, 1, struct agent_ioctl_add_dev)

#define AGENT_DEL_DEVICE _IOW(AGENT_IOCTL_BASE, 2, struct agent_ioctl_del_dev)

#endif
