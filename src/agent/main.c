#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include "log.h"
#include "blk_dev.h"
#include "ctrl_dev.h"

static int agent_main_init(void)
{
    int ret;
    LOG_INFO("hello agent");
    ret = pbdev_mgr_init(&g_dev_mgr);
    if(ret){
        LOG_ERR("dev mgr init failed");
        return ret;
    }

    ret = ctrl_dev_init();
    if(ret){
        LOG_ERR("ctrl dev init failed");
        return ret;
    }
    return 0;
}

static void agent_main_exit(void)
{
    LOG_INFO("goodbyte agent");
    ctrl_dev_exit();
    pbdev_mgr_fini(&g_dev_mgr);
}

module_init(agent_main_init);
module_exit(agent_main_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("levi");
MODULE_DESCRIPTION("storage gateway agent");
