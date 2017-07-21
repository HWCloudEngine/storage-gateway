#!/bin/bash

cd /sys/fs/cgroup
mkdir blkio
mount -t cgroup -o blkio blkio /sys/fs/cgroup/blkio
cd blkio
#cgroup group
mkdir test1
#8:16 is block device major/minor num, 200 is expected iops
echo "8:16 200" > /sys/fs/cgroup/blkio/test1/blkio.throttle.write_iops_device
apt-get install cgroup-bin
cgexec -g blkio:test1 fio -filename=/dev/sdb -direct=1 -iodepth=32 -rw=write -ioengine=libaio -bs=1M -size=10G -numjobs=1 -runtime=1000 -group_reporting -name=test
#use iotop  to see iops
apt-get install iotop
iotop -k

