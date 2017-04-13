#ifndef CTRL_DEV_H
#define CTRL_DEV_H
#include "../common/agent_ioctl.h"
#include <linux/limits.h>

int ctrl_dev_init(void);
void ctrl_dev_exit(void);

#endif
