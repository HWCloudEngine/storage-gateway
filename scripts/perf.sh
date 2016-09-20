#!/bin/bash

#fio parameter
device="/dev/sdc"
direct=1
access_set="write randwrite read randread"
ioengine="sync"
iodepth=1
block_size="8k"
size="30M"
runtime="20s"

#test round time, then calculate average
round=5

#iops test
function iops_test {
    for access_op in ${access_set}
    do
        ips_sum=0
        ips_avg=0
        for(( i=0; i < $round; i++))
        do
            result=$(fio -filename=${device}\
                --direct=1\
                -rw=${access_op}\
                -ioengine=${ioengine}\
                -iodepth=${iodepth}\
                -bs=${block_size}\
                --runtime=${runtime}\
                -name=test\
                | grep "iops"\
                | awk -F, '{print $3}'\
                | awk -F= '{print $2}')

            ips_sum=$(echo $ips_sum $result | awk '{print $1 + $2}')
        done
        ips_avg=$(echo $ips_sum $round | awk '{printf $1 / $2}')
        printf "%-4s %-10s iops:%s \n" "iops" ${access_op} ${ips_avg}
    done
}

#latency test
function lat_test {
    for access_op in ${access_set}
    do
        lat_sum=0
        lat_avg=0
        for(( i=0; i<$round; i++))
        do
            result=$(fio -filename=${device}\
                --direct=1\
                -rw=${access_op}\
                -ioengine=${ioengine}\
                -iodepth=${iodepth}\
                -bs=${block_size}\
                --runtime=${runtime}\
                -name=test\
                | grep "avg="\
                | grep "clat"\
                | awk -F, '{print $3}'\
                | awk -F= '{print $2}')

            lat_sum=$(echo $lat_sum $result | awk '{print $1 + $2}')
        done
        lat_avg=$(echo $lat_sum $round | awk '{print $1 / $2}')

        printf "%-4s %-10s avg:%s \n" "lat" ${access_op} ${lat_avg}
    done
}

case $1 in
"iops")
    iops_test;;
"lat")
    lat_test;;
*)
    echo "command no support"
esac
