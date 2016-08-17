/**************************************************
* copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    test.cc
* Author: 
* Date:         2016/08/13
* Version:      1.0
* Description:
* Usage: ./test_program [--log_level=] [--run_test=]
*     --log_level,Acceptable Values:    
*      all            - report all log messages including the passed test notification
*      success      - the same as all
*      test_suite     - show test suite messages
*      message      - show user messages
*      warning        - report warnings issued by user
*      error            - report all error conditions
*      cpp_exception  - report uncaught c++ exception
*      system_error - report system originated non-fatal errors (for example, timeout or floating point exception)
*      fatal_error    - report only user or system originated fatal errors (for example, memory access violation)
*      nothing        - does not report any information
*
*     --run_test, set test unit filter, for example:
*      --run_test=case1                       run test cases named "case1"
*      --run_test=writer_client_test/case1    run test cases named "case1" in test suit "writer_client_test"
*      --run_test=case1,case2                 run test cases named "case1" and "case2"
*      --run_test=case*                       run test cases named with prefix="case";
*      --run_test=writer_client_test          run all test cases within test suit "writer_client_test"
***********************************************/
#define BOOST_TEST_DYN_LINK // use dynamic boost libraries
    // BOOST_TEST_MODULE macro auto implement initialization function and name the main test suite
#define BOOST_TEST_MODULE test
#include <boost/test/included/unit_test.hpp>
