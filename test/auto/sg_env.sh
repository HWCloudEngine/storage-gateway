#!/bin/bash
export SG_SOURCE_HOME=/opt/workspace/storage-gateway
export SG_SCRIPTS_HOME=${SG_SOURCE_HOME}/test/auto
export SG_CTRL_SCRIPT_HOME=${SG_SOURCE_HOME}/scripts/ctrl_commands
export LOCAL_HOST="162.3.111.162"
export REMOTE_HOST="162.3.111.152"
export CLIENT_HOST_1="162.3.153.130"
export CLIENT_HOST_2="162.3.153.132"
export PRIMARY_VOLUME="test_volume"
export SECONDARY_VOLUME="test_volume_1"
export PRIMARY_ROLE="primary"
export SECONDARY_ROLE="secondary"
export TGT_SRC_HOME="/root/tgt"
#iscsi or agent
export CLIENT_MODE="agent"
