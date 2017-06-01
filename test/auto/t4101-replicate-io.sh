#!/bin/sh
#
# Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
#

test_description='4101 test ()

    This test write some data, read it, and compare them.'

# include sharness lib
. ./lib/sharness/sharness.sh

# get curret work path
AUTO_HOME=${AUTO_HOME:-$(cd $(dirname $0)/..;pwd)}

# todo
test_expect_success 'io test' '
    cd ${AUTO_HOME}

'


test_done
