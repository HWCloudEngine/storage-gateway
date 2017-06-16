#!/bin/bash
CUR_PATH=${CUR_PATH:-$(cd $(dirname $0);pwd)}
web_root=/opt/jenkins/coverage
coverage_dir=coverage
rm $coverage_dir -rf
mkdir -p $coverage_dir
##reset coverage counters
find $CUR_PATH/src -name *.gcda | xargs rm -f
##run unit_test
make check
#source_files=`find $CUR_PATH/src -name *.cc`
#gcov -r $source_files
##colect coverage counters
lcov --no-external -c -d ./src -o $coverage_dir/coverage.info
rm $web_root/storage-gateway/* -rf
genhtml $coverage_dir/coverage.info -o $web_root/storage-gateway -t storage-gateway-coverage
