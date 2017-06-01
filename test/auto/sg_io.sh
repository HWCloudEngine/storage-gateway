#!/bin/bash
# start or stop sg services

source ./sg_env.sh
. ${SG_SCRIPTS_HOME}/sg_functions.sh



#iscsi <attach|detach> <$TARGET_HOST>
iscsi(){
    if [ $2"a" == "a" ];then
        return 1
    fi
    target_host=$2
    res1=`iscsiadm -m discovery -t sendtargets -p $target_host`
    #expect "162.3.153.129:3260,1 iqn.2017-01.huawei.sgs.jenkins_voume_1"
    if [[ $? -ne 0 ]]; then
        sg_print "discover targets from host failed."
        sg_print "iscsiadm: $res1"
        return 1
    fi
    sg_print "discovery targtets: $res1"

    TARGET=`echo $res1 | awk '{print $2}'`
    case $1 in
        "attach")
        sg_print "try to attach target: $target_host"
        res2=`iscsiadm -m node -T $TARGET -p $target_host --login`
        if [[ $? -ne 0 ]]; then
           sg_print "iscsiadm login [$target_host] target failed!"
           return 2 
        fi
        ;;
        "detach")
        sg_print "try to detach target: $1"
        res2=`iscsiadm -m node -T $TARGET -p $target_host --logout`
        if [[ $? -ne 0 ]]; then
           sg_print "iscsiadm logout [$target_host] target failed!"
           return 2
        fi
        sg_print "detach done!"
        iscsiadm -m node -o delete $TARGET -p $target_host
        if [[ $? -ne 0 ]]; then
           sg_print "iscsiadm delete [$target_host] target failed!"
           return 3
        fi
        ;;
    *)
    esac
}

#get_disk_symbol $TARGET_HOST $symbol_file
get_disk_symbol()
{
    DISK_PREFIX=/dev/
    rm -f /tmp/pre_disks /tmp/cur_disks
    rm -f $2
    #logout
    iscsi "detach" $1
    sleep 2
    #save disk list
    lsblk -d -n -o NAME|sort > /tmp/pre_disks
    iscsi "attach" $1
    error_code=$?
    if [ $? -ne 0 ] ; then
        sg_print "attach iscsi disk failed:$error_code !"
        return $error_code 
    fi

    sleep 2
    #read new disk list
    lsblk -d -n -o NAME|sort > /tmp/cur_disks

    #get the diff
    new_attached_disk=$(comm -13 /tmp/pre_disks /tmp/cur_disks)
    #trim
    new_attached_disk=$(echo $new_attached_disk)
    if test -z $new_attached_disk; then
        sg_print "attached disk not found!"
    fi
#    rm -f /tmp/pre_disks /tmp/cur_disks
    echo "${DISK_PREFIX}${new_attached_disk}"|sed 's/^ //g' > $2
}

#write_file $file $disk
write_file()
{
    if test -z $2; then
        return 1
    fi
    rm -f $file
    res=$(dd if=$1 of=$2 bs=4096 oflag=direct) 
    if [ $? -ne 0 ]; then
        return 1
    fi
    sg_print "write result: $res"
}

#read_io $size $disk $file, read $size bytes data from $disk and save to $file
read_file()
{
    if test -z $2; then
        return 1
    fi
    rm -f $3
    disk=$(echo $2 | sed 's/^ //g')
    let blocks=$1/4096
    if [[ $disk == "/dev/urandom" ]]; then
        res=$(dd if=$disk of=$3 bs=4096 count=$blocks)
    else
        res=$(dd if=$disk of=$3 bs=4096 iflag=direct count=$blocks)
    fi

    if [ $? -ne 0 ]; then
        sg_print "read result: $res"
        return 1
    fi
}

usage="usage: $0 <local|remote> <local_host:local io test> [remote_host:required if test replication]"
if [ $# -lt 2 ];then
    echo $usage
    exit 1
fi
if [[ $2 == "remote" && a"$3" == "a" ]]; then
   echo $usage
   exit 1
fi
sg_print "$@"
LOCAL_TARGET_HOST=$2
REMOTE_TARGET_HOST=$3

disk_symbol_file="/tmp/sg_disk_symbol";
local_disk_symbol_file="/tmp/sg_disk_local";
remote_disk_symbol_file="/tmp/sg_disk_remote";
file_to_write="/tmp/file_to_write" #100M
file_written="/tmp/file_written"
file_size=104857600
sg_print "start io test on $CLIENT_HOST_1"
#attach iscsi volume,ant get attached disk symbol
sg_print "attach sg volume"
ssh $CLIENT_HOST_1 "$(typeset -f );get_disk_symbol $LOCAL_TARGET_HOST $local_disk_symbol_file"
error_code=$?
if [ $error_code -ne 0 ] ; then
    sg_print "get disk symbol failed:$error_code"
    ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
    exit 1
fi
#prepare test file
sg_print "prepare test file"
ssh $CLIENT_HOST_1 "test -r $file_to_write"
if [ $? -ne 0 ]; then
    ssh $CLIENT_HOST_1 "$(typeset -f); read_file $file_size /dev/urandom $file_to_write "
    if [ $? -ne 0 ] ; then
      sg_print "prepare test file failed"
      ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
      exit 1
    fi
fi
#write test file
sg_print "write test file to sg volume"
ssh $CLIENT_HOST_1 "$(typeset -f); disk=\$(sed -n '1p' $local_disk_symbol_file); \
    write_file $file_to_write \$disk"
if [ $? -ne 0 ] ; then
    sg_print "write test file failed"
    ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
    exit 1
fi

#if test replication,read from remote target
if [[ $1 == "remote" ]]; then
    #todo: continue when replication done?
    sleep 10
    #attach iscsi volume,ant get attached disk symbol
    sg_print "attach remote sg volume"
    ssh $CLIENT_HOST_1 "$(typeset -f );get_disk_symbol $REMOTE_TARGET_HOST $remote_disk_symbol_file"
    error_code=$?
    if [ $error_code -ne 0 ] ; then
        sg_print "get remote disk symbol failed:$error_code"
        ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $REMOTE_TARGET_HOST"
        ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
        exit 1
    fi
    #read written file for comparation
    ssh $CLIENT_HOST_1 "$(typeset -f); disk=\$(sed -n '1p' $remote_disk_symbol_file); \
        read_file $file_size \$disk $file_written"
    if [ $? -ne 0 ] ; then
        sg_print "read written file from remote target failed."
        ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
        ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $REMOTE_TARGET_HOST"
        exit 1
    fi
    #ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
    ##ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $REMOTE_TARGET_HOST"
else
    #read written file for comparation
    ssh $CLIENT_HOST_1 "$(typeset -f); disk=\$(sed -n '1p' $local_disk_symbol_file); \
        read_file $file_size \$disk $file_written"
    if [ $? -ne 0 ] ; then
        sg_print "read written file failed."
        ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
        exit 1
    fi

    #ssh $CLIENT_HOST_1 "$(typeset -f ); iscsi "detach" $LOCAL_TARGET_HOST"
fi

#compare test file
sg_print "compare the files"
ssh $CLIENT_HOST_1 "cmp $file_to_write $file_written"
if [ $? -ne 0 ] ; then
    sg_print "the file written is inconsistent with test file!!!"
    ssh $CLIENT_HOST_1 "$(typeset -f); iscsi "detach" $LOCAL_TARGET_HOST"
    exit 1
fi
