#!/bin/bash
# test for volume enable/disable

source ./sg_env.sh
. ${SG_SCRIPTS_HOME}/sg_functions.sh
 
usage="usage: $0 [action:enable|disable|attach|detach|initialize|terminate] [volume id] "
TEST_VOLUME_PATH="/dev/sdc"
#1G
TEST_VOLUME_SIZE=1073741824
if [ $# -lt 2 ]
then 
sg_print $usage
exit 1
fi
TEST_VOLUME_ID=$2

cd ${SG_CTRL_SCRIPT_HOME}
#prepare python scripts
#./build_grpc.sh
cd sg_control

case $1 in
    'enable')
        sg_print "enable volume ${TEST_VOLUME_ID}"
        res=$(python control.py volume -v ${TEST_VOLUME_ID} -d ${TEST_VOLUME_PATH} -s ${TEST_VOLUME_SIZE} -a enable)
        sg_print "$res"
        if ! is_substr "result:0" "$res" ; then
           sg_print "enable volume failed!"
           exit 1
        fi
        ;;
    'disable')
        sg_print "disable volume ${TEST_VOLUME_ID}"
        res=$(python control.py volume -v ${TEST_VOLUME_ID} -a disable)
        sg_print "$res"
        if ! is_substr "result:0" "$res" ; then
           sg_print "disable volume failed!"
           exit 1
        fi 
        ;;
    'attach')
        # todo: get device path
        sg_print "attach volume ${TEST_VOLUME_ID} $TEST_VOLUME_PATH"
        res=$(python control.py volume -v ${TEST_VOLUME_ID} -d ${TEST_VOLUME_PATH} -a attach)
        sg_print "$res"
        if ! is_substr "result:0" "$res" ; then
           sg_print "attach volume failed!"
           exit 1
        fi 
        ;;
    'detach')
        sg_print "detach volume ${TEST_VOLUME_ID}"
        res=$(python control.py volume -v ${TEST_VOLUME_ID} -a detach)
        sg_print "$res"
        if ! is_substr "result:0" "$res" ; then
           sg_print "detach volume failed!"
           exit 1
        fi 
        ;;
    'initialize')
        sg_print "initialize volume ${TEST_VOLUME_ID}"
        res=$(python control.py volume -v ${TEST_VOLUME_ID} -m 'iscsi' -a initialize)
        sg_print "$res"
        if ! is_substr "result:0" "$res" ; then
           sg_print "initialize volume failed!"
           exit 1
        fi 
        ;;
    'terminate')
        sg_print "terminate volume ${TEST_VOLUME_ID}"
        res=$(python control.py volume -v ${TEST_VOLUME_ID} -a terminate)
        sg_print "$res"
        if ! is_substr "result:0" "$res" ; then
           sg_print "terminate volume failed!"
           exit 1
        fi 
        ;;
    *)
        sg_print "$0 $1 not support"
        exit 1
        ;;
esac
