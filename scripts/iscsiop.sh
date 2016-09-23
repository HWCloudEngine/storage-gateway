#!/bin/bash

target_host="162.3.111.109"

target=$(iscsiadm -m discovery -t sendtargets -p $target_host | awk '{print$2}')

case $1 in
'login')
    echo "<<<< do login:" ${target}
    iscsiadm -m node -T ${target} --login;;
'logout')
    echo "<<<< do logout:" ${target}
    iscsiadm -m node -T ${target} --logout;;
*)
    echo $1, "no support current";;
esac
