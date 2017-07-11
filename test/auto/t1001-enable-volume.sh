#!/bin/sh
#
# Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
#

test_description='1001 test ()

    This test enable a test volume on each host.'

# include sharness lib
. ./lib/sharness/sharness.sh

# get curret work path
AUTO_HOME=${AUTO_HOME:-$(cd $(dirname $0)/..;pwd)}

# set sg env
. ${AUTO_HOME}/sg_env.sh

test_expect_success 'enable a test volume' '
    if [ "$CLIENT_MODE"x = "iscsi"x ]; then
        cd ${AUTO_HOME} &&
        ./sg_test.sh enable &&
        ./sg_test.sh initialize
    else
       cd ${AUTO_HOME} &&
        ./sg_test.sh enable &&
        ./sg_test.sh attach
   fi
'

test_done
