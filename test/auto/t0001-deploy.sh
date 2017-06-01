#!/bin/sh
#
# Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
#

test_description='0001 test ()

    This test download sg source and build it.'

# include sharness lib
. ./lib/sharness/sharness.sh

# get curret work path
AUTO_HOME=${AUTO_HOME:-$(cd $(dirname $0)/..;pwd)}

# set sg env
#source ${AUTO_HOME}/sg_env.sh

test_expect_success 'deploy storage-gateway' '
    cd ${AUTO_HOME}
    ./sg_deploy.sh
'

test_done
