#!/bin/bash

storage_gateway_src_path="/root/storage-gateway/src"
tgt_src_path="/root/tgt"
server_ip="127.0.0.1"
server_port=9999
unix_domain_path="/var/local_pipe"

case $1 in
    'start')
        echo "do start"
        export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
        cd ${storage_gateway_src_path}/sg_server
        ulimit -c unlimited
        ./sg_server &
        cd ${storage_gateway_src_path}/sg_client
        ulimit -c unlimited
        ./sg_client ${server_ip} ${server_port} ${unix_domain_path} &
        #cd ${tgt_src_path}/usr
        #ulimit -c unlimited
        #tgtd -f &
        #/etc/init.d/tgt start
        service tgt start 
        ;;
    'stop')
        echo "do stop"
        #/etc/init.d/tgt stop
        service tgt stop
        pgrep -f tgtd | xargs kill -9
        pgrep -f sg_client | xargs kill -9
        pgrep -f sg_server | xargs kill -9
        ;;
    *)
        echo "not support"
        ;;
esac
