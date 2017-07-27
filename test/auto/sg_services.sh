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
            sg_print "start sg_server ok "
        fi
        
        # install agent driver
        if [[ "${CLIENT_MODE}" = "agent" ]]; then 
            lsmod | grep sg_agent
            if [ $? -eq 1 ]
            then
                cd ${SG_SOURCE_HOME}/src/agent
                insmod sg_agent.ko
            fi
            sg_print "install sg_agent ok "
        fi

        if [ "$sg_client_pid" = "" ]; then
            cd ${SG_SOURCE_HOME}/src/sg_client
            ulimit -c unlimited
            nohup ./sg_client &
            sg_print "start sg_client ok "
            sleep 2 
        fi

        if [[ "${CLIENT_MODE}" = "iscsi" &&
              "$tgt_status" = "tgt stop/waiting" ]]; then
            nohup /etc/init.d/tgt start &
            sg_print "start tgt ok "
            sleep 1
        fi

        #check & verify process exsit
        sg_print "verify all services are running"
        sg_server_pid=`pgrep sg_server`
        sg_client_pid=`pgrep sg_client`
        if [[ "$sg_server_pid" = ""  || "$sg_client_pid" = "" ]]; then
            sg_print "start sg services failed!"
            exit 1
        fi
        sg_print "start sg done!"
        exit 0
        ;;

    'stop')
        sg_print "do stop "
        if [[ ${CLIENT_MODE} = "iscsi" ]]; then
            /etc/init.d/tgt stop
            sg_print "stop tgt ok "
        fi
        
        if [[ ${CLIENT_MODE} = "agent" && $(pgrep -f sg_client) != "" ]]; then
            ./volume.sh detach $2
            ./volume.sh terminate $2
            ./volume.sh disable $2
            sleep 10
            sg_print "do stop disable volume " $pwd $2
        fi
        pgrep -f tgtd | xargs kill -9
        pgrep -f sg_client | xargs kill -9
        pgrep -f sg_server | xargs kill -9
        sg_print "do stop kill sg_client sg_server"
        
        lsmod | grep sg_agent
        if [[ $? == 0 ]]; then
            rmmod sg_agent
            sg_print "do stop remove agent"
        fi
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
          rm /etc/tgt/conf.d/${TEST_VOLUME_ID} -f
        fi
        rm /var/storage_gateway/* -rf
        > /etc/storage-gateway/agent_dev.conf
        # delete bucket
        ${SG_SOURCE_HOME}/test/journal_meta_utils delete
        ;;
    *)
        sg_print "$0 $1 not support"
        exit 1
        ;;
esac
