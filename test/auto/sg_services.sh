#!/bin/bash
# start or stop sg services

source ./sg_env.sh
. ${SG_SCRIPTS_HOME}/sg_functions.sh

if [ $2 ]
then
  TEST_VOLUME_ID=$2
fi

case $1 in
    'start')
        sg_print "do start"
        export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
        sg_server_pid=`pgrep sg_server`
        sg_client_pid=`pgrep sg_client`
        tgt_status=`/etc/init.d/tgt status`
        if [ "$sg_server_pid" = "" ]; then
            cd ${SG_SOURCE_HOME}/src/sg_server
            ulimit -c unlimited
            nohup ./sg_server &
        fi
        
        # install agent driver
        lsmod | grep sg_agent
        if [ $? -eq 1 ]; then
            cd ${SG_SOURCE_HOME}/src/agent
            insmod sg_agent.ko
        fi

        if [ "$sg_client_pid" = "" ]; then
            cd ${SG_SOURCE_HOME}/src/sg_client
            ulimit -c unlimited
            nohup ./sg_client ${CTRL_SERVER_HOST} ${CTRL_SERVER_PORT} ${UNIX_DOMAIN_PATH} &
            sleep 2 
        fi

        if [ "$tgt_status" = "tgt stop/waiting" ]; then
            nohup /etc/init.d/tgt start &
            sleep 1
        fi

        #check & verify process exsit
        sg_print "verify all services are running"
        sg_server_pid=`pgrep sg_server`
        sg_client_pid=`pgrep sg_client`
        tgt_status=`/etc/init.d/tgt status`
        if [[ "$sg_server_pid" = ""  || 
            "$sg_client_pid" = "" || 
            "$tgt_status" = "tgt stop/waiting" ]]; then
            sg_print "start sg services failed!"
            exit 1
        fi
        # remove agent driver
        lsmod | grep sg_agent
        if [ $? -eq 0 ]; then
            rmmod sg_agent
        fi

        sg_print "start sg done!"
        exit 0
        ;;

    'stop')
        sg_print "do stop "
        /etc/init.d/tgt stop
        pgrep -f tgtd | xargs kill -9
        pgrep -f sg_client | xargs kill -9
        pgrep -f sg_server | xargs kill -9
        ;;

    'clean')
        sg_print "do clean `date` "
        rm /var/log/storage-gateway/* -f
        rm ${SG_SOURCE_HOME}/sg_client/sg_client.log* -f
        rm ${SG_SOURCE_HOME}/sg_server/sg_server.log* -f
        rm ${SG_SOURCE_HOME}/sg_client/core -f
        rm ${SG_SOURCE_HOME}/sg_server/core -f
        if [ ! -z ${TEST_VOLUME_ID} ]
        then
          rm /mnt/cephfs/journals/${TEST_VOLUME_ID}/* -rf
          rm /var/tmp/${TEST_VOLUME_ID}/ -rf
          rm /etc/tgt/conf.d/${TEST_VOLUME_ID} -f
        fi
        # delete bucket
        ${SG_SOURCE_HOME}/test/libs3_utils delete
        ;;
    *)
        sg_print "$0 $1 not support"
        exit 1
        ;;
esac
