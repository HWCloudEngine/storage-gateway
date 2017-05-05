#!/bin/bash
source ./sg_env.sh
. ${SG_SCRIPTS_HOME}/sg_functions.sh

usage="usage: $0 [1:action:enable|create_rep|enable_rep|disable_rep|failover_rep|delete_rep]\n
          [2:operation_id,required] \n
          [3:role:primary|secondary,required] \n
          [4:local_volume_id, required] \n
          [5:peer_volume_id, required for create] \n"
#          [5:snap_id, required for failover] \n
#          [6:checkpoint_id, required for failover] \n"
sg_print "$0 : params_c=$#"

replicate_uuid="test_replicate_uuid"
#default role
rep_role="primary"
if [ $# -lt 4 ]
then
    sg_print -e $usage
exit 1
fi

operate_uuid=$2
rep_role=$3
volume_id=$4
#peer_volume=$5

cd ${SG_CTRL_SCRIPT_HOME}
#prepare python scripts
#./build_grpc.sh
cd sg_control

case $1 in
    'create_rep')
        if [ $# -lt 5 ]
        then
            sg_print $usage
            sg_print $usage
        fi
        peer_volume=$5
        sg_print "create_rep"
        res=`python control.py replicate -a create -i ${replicate_uuid} -o ${operate_uuid} -v ${volume_id} --second_volume ${peer_volume} -r ${rep_role}`
        ;;
    'enable_rep')
        sg_print "enable replication"
        res=`python control.py replicate -a enable -o ${operate_uuid} -v ${volume_id}  -r ${rep_role}`
        ;;
    'disable_rep')
        sg_print "disable replication"
        res=`python control.py replicate -a disable -o ${operate_uuid} -v ${volume_id}  -r ${rep_role}`
        ;;
    'failover_rep')
        sg_print "failover replication"
        checkpoint_id=${operate_uuid}
        snap_id=`cat /proc/sys/kernel/random/uuid`
        res=`python control.py replicate -a failover -o ${operate_uuid} -v ${volume_id} -r ${rep_role} -s ${snap_id} -c ${checkpoint_id}`
        ;;
    'delete_rep')
        sg_print "delete replication"
        res=`python control.py replicate -a delete -o ${operate_uuid} -v ${volume_id}  -r ${rep_role}`
        ;;
    'reverse_rep')
        sg_print "reverse replication"
        res=`python control.py replicate -a reverse -o ${operate_uuid} -v ${volume_id}  -r ${rep_role}`
        ;;
    *)
        sg_print "$0 $1 not support"
        exit 1
        ;;
esac
    sg_print "$res"
    if ! is_substr "result:0" "$res" ; then
        sg_print "rep operation $1 failed!"
        exit 1
    fi
