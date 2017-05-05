#!/bin/bash
export SG_SOURCE_HOME=/opt/storage-gateway
export SG_SCRIPTS_HOME=${SG_SOURCE_HOME}/test/auto
export SG_CTRL_SCRIPT_HOME=${SG_SOURCE_HOME}/scripts/ctrl_commands
export LOCAL_HOST="162.3.153.128"
export REMOTE_HOST="162.3.153.129"
export CLIENT_HOST_1="162.3.153.130"
export CLIENT_HOST_2="162.3.153.132"
export PRIMARY_VOLUME="jenkins_volume"
export SECONDARY_VOLUME="jenkins_voume_1"
export PRIMARY_ROLE="primary"
export SECONDARY_ROLE="secondary"
export TGT_SRC_HOME="/root/tgt"
export CTRL_SERVER_HOST="127.0.0.1"
export CTRL_SERVER_PORT=9999
export UNIX_DOMAIN_PATH="/var/local_pipe"
