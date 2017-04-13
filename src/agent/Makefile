KDIR :=/lib/modules/$(shell uname -r)/build
PWD  :=$(shell pwd)

obj-m := sg_agent.o
sg_agent-objs := main.o ctrl_dev.o blk_dev.o transport.o

default:
	$(MAKE) -C $(KDIR) SUBDIRS=$(PWD) modules

clean:
	rm -rf *.o *.ko *.mod.* .*.o.cmd .*.ko.cmd modules.order Module.symvers
