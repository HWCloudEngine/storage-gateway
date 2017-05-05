#!/bin/sh
#
# Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
#

test_description='1001 test ()

    This test disable the replication.'

# include sharness lib
. ./lib/sharness/sharness.sh

# get curret work path
AUTO_HOME=${AUTO_HOME:-$(cd $(dirname $0)/..;pwd)}

# set sg env
#source ${AUTO_HOME}/sg_env.sh

test_expect_success 'disable replication' '
    cd ${AUTO_HOME} &&
    ./sg_test.sh disable_rep
'

test_done
