#!/bin/sh
#
# Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
#

test_description='9009 test ()

    This test stop sg processes: sg_client, sg_server and tgt service.'

# include sharness lib
. ./lib/sharness/sharness.sh

# get curret work path
AUTO_HOME=${AUTO_HOME:-$(cd $(dirname $0)/..;pwd)}

# set sg env
#source ${AUTO_HOME}/sg_env.sh


test_expect_success 'stop sg services' '
    cd ${AUTO_HOME} &&
    ./sg_test.sh stop    
'
test_done
