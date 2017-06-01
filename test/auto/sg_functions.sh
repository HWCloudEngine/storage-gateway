#!/bin/sh

SG_LOG_HOME=/var/log/storage-gateway

sg_print()
{
#    printf "$@" >> ${SG_LOG_HOME}/$(date +"%Y_%m_%d").log
#    printf "\n" >> ${SG_LOG_HOME}/$(date +"%Y_%m_%d").log
     echo -n "`date` :" >> ${SG_LOG_HOME}/$(date +"%Y_%m_%d").log
     echo "$@" >> ${SG_LOG_HOME}/$(date +"%Y_%m_%d").log
     echo "$@"
     sync
}

# check if B is substr of A
# usage: is_substr B A
is_substr()
{
    success=$(echo "$2" | grep -E "$1")
    if [[ $success == "" ]]; then
        return 1
    fi
    return 0
}
