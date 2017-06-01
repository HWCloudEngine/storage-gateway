#!/bin/bash

source ./sg_env.sh
. ${SG_SCRIPTS_HOME}/sg_functions.sh

deploy()
{
    if [ $# -lt 1 ]; then
        sg_print "params $# error,deploy failed!"
        return 1
    fi
    SG_HOST=$1
    TEMP=${SG_SOURCE_HOME%/storage-gateway*}
    #0. clean environment
    sg_print "clean: to stop sg service & delete source"
    ssh $SG_HOST "test -d ${SG_SOURCE_HOME}"
    if [ $? -eq 0 ]; then
        sg_print "stop sg service failed!"
        ssh -t $SG_HOST "cd ${SG_SCRIPTS_HOME}; ./sg_services.sh stop; ./sg_services.sh clean"
        sleep 5
        ssh $SG_HOST "rm -rf ${SG_SOURCE_HOME}"
        if [ $? -ne 0 ]; then
            sg_print "delete source code failed!"
        fi
    else
        sg_print "clean : source file not exist."
    fi
    # check tgt
    sg_print "clean: to stop tgt & delete tgt source"
    ssh $SG_HOST "test -d ${TEMP}/tgt"
    if [ $? -eq 0 ]; then
        ssh $SG_HOST "service tgt stop"
        sg_print -rf "delete tgt source file"
        ssh $SG_HOST "rm -rf ${TEMP}/tgt"
    else
        sg_print "clean : tgt source file not exist."
    fi
    #1. download source code
    sg_print "clone source to $TEMP"
    ssh $SG_HOST "cd ${TEMP}; git clone https://github.com/Hybrid-Cloud/storage-gateway.git"
    if [ $? -ne 0 ]; then
        sg_print "download source code failed!"
        return 1
    fi

    #2. build source code
    sg_print "build source code"
    ssh $SG_HOST "cd ${SG_SOURCE_HOME}; ./build.sh > /dev/null"
    ssh $SG_HOST "cd ${SG_SOURCE_HOME}/src/agent; make clean && make > /dev/null"
    if [ $? -ne 0 ]; then
        sg_print "build source code failed!"
        return 1
    fi

    #3. build tgt 
    sg_print "download tgt source code"
    ssh $SG_HOST "cd ${TEMP}; git clone https://github.com/Hybrid-Cloud/tgt.git -b iohook"
    if [ $? -ne 0 ]; then
        sg_print "download tgt source code failed!"
        return 1
    fi
    ssh $SG_HOST "cd ${TEMP}/tgt; make & make install"
    if [ $? -ne 0 ]; then
        sg_print "build tgt failed!"
        return 1
    fi

    #4. generate ctrl scripts
    ssh $SG_HOST "cd ${SG_CTRL_SCRIPT_HOME}; chmod +x build_grpc.sh; ./build_grpc.sh > /dev/null"
    if [ $? -ne 0 ]; then
        sg_print "generate control script failed!"
        return 1
    fi

    #5. copy auto test scripts
    sg_print "delpy test scripts"
    scp -r ../auto ${SG_HOST}:${SG_SCRIPTS_HOME%auto}
    if [ $? -ne 0 ]; then
        sg_print "copy scripts failed!"
        return 1
    fi
    ssh ${SG_HOST} "cd ${SG_SCRIPTS_HOME}; chmod +x *.sh"

#    #6. start sg services
#    sg_print "start sg services"
#    # use "-t" to avoid hanging up
#    ssh -t ${SG_HOST} "cd ${SG_SCRIPTS_HOME}; ./sg_services.sh start"
#    if [ $? -ne 0 ]; then
#        sg_print "start sg services failed!"
#        return 1
#    fi
#    #make sure sg services running, wait a few seconds
#    sleep 5
}

sg_print "start to deploy storage-gateway on ${LOCAL_HOST}"
deploy ${LOCAL_HOST}
if [ $? -ne 0 ]; then
    sg_print "deploy on ${LOCAL_HOST} failed!"
    exit 1
fi
sg_print "start to deploy storage-gateway on ${REMOTE_HOST}"
deploy ${REMOTE_HOST}
if [ $? -ne 0 ]; then
    sg_print "deploy on ${REMOTE_HOST} failed!"
    exit 1
fi
