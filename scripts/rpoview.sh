#!/bin/bash

#max replicator producer and consumer mark
#rpo time = rpo_value / 1024 / write_bw
rpo_value=0
#max journal file size(default 32MB)
readonly max_file_size=33554432

while true
do
    #get replicator consumer marker
    res0=$(./libs3_utils get /markers/test_volume/consumer/replicator)
    con_journal=$(echo $res0 | awk '{print $3}' | awk -F/ '{print $4}')
    con_offset=$(echo $res0 | awk '{print $5}')
    con_journal=${con_journal:12:4}
    echo "consumer:" $con_journal $con_offset

    #get replicator producer marker
    res1=$(./libs3_utils get /markers/test_volume/producer/replicator)
    pro_journal=$(echo $res1 | awk '{print $3}' | awk -F/ '{print $4}')
    pro_offset=$(echo $res1 | awk '{print $5}')
    pro_journal=${pro_journal:12:4}
    echo "producer:" $pro_journal $pro_offset

    if [ $con_journal = $pro_journal  -a $pro_offset = $con_offset ]
    then
        echo "same journal same pos:" $pro_offset " " $con_offset
    elif [ $con_journal = $pro_journal -a $pro_offset != $con_offset ]
    then
        #calculate producer and consumer gap
        cur_gap=`expr $pro_offset - $con_offset`
        if [ $cur_gap -gt $rpo_value ]
        then
            rpo_value=$cur_gap
        fi
        echo "some journal diff con_pos:" $con_offset "pro_pos:" $pro_offset
    else
        #calculate producer and consumer gap
        pro_file_ino=$(printf "%d\n" 0x$pro_journal)
        con_file_ino=$(printf "%d\n" 0x$con_journal)
        file_ino=`expr $pro_file_ino - $con_file_ino - 1`
        file_ino=$(printf "%d\n" 0x$file_ino)
        con_file_gap=`expr $max_file_size - $con_offset`
        inv_file_gap=`expr $file_ino \* $max_file_size`
        echo "inv_file_gap:" $inv_file_gap
        pro_file_gap=$pro_offset
        cur_gap=`expr $con_file_gap + $inv_file_gap + $pro_file_gap`
        echo "diff journal diff file_num:" $file_ino "con_pos: " $con_offset \
             "pro_pos:" $pro_offset "cur_gap:" $cur_gap
        if [ $cur_gap -gt $rpo_value ]
        then
            rpo_value=$cur_gap
        fi
    fi

    echo "max rpo_value:" $rpo_value
    sleep 5
done
