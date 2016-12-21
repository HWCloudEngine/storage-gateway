#!/bin/sh
USAGE="$0 [make | clean] \n option: -debug, enable debug mode"
CUR_PATH=$(/bin/pwd)
RPC_SOURCE_PATH=${CUR_PATH}/src/rpc
PROTO_PATH=${CUR_PATH}/src/rpc/protos
TEST_PATH=${CUR_PATH}/src/test
PROTOC=$(which protoc)
if [ 0 -eq $# ] || [ "$1"a = "make"a ]
then
#generate grpc C++ source files
echo "rm old grpc source files..."
rm -f ${RPC_SOURCE_PATH}/*.pb.h ${RPC_SOURCE_PATH}/*.pb.cc
echo "generate new grpc C++ source files to ${RPC_SOURCE_PATH} ..."

#generate protocol
$PROTOC -I ${PROTO_PATH} \
        --cpp_out=$RPC_SOURCE_PATH \
        ${PROTO_PATH}/common.proto \
        ${PROTO_PATH}/journal/journal.proto \
        ${PROTO_PATH}/journal/message.proto \
        ${PROTO_PATH}/journal/writer.proto \
        ${PROTO_PATH}/journal/consumer.proto \
        ${PROTO_PATH}/inner_command/snapshot.proto \
        ${PROTO_PATH}/inner_command/replicate_inner_control.proto \
        ${PROTO_PATH}/replication/replicator.proto \
        ${PROTO_PATH}/control_api/control.proto \
        ${PROTO_PATH}/control_api/replicate_control.proto
#generate grpc
$PROTOC -I ${PROTO_PATH} \
        --grpc_out=$RPC_SOURCE_PATH \
        --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
        ${PROTO_PATH}/journal/writer.proto \
        ${PROTO_PATH}/journal/consumer.proto \
        ${PROTO_PATH}/inner_command/snapshot.proto \
        ${PROTO_PATH}/inner_command/replicate_inner_control.proto \
        ${PROTO_PATH}/replication/replicator.proto \
        ${PROTO_PATH}/control_api/control.proto \
        ${PROTO_PATH}/control_api/replicate_control.proto
#make soft-link of source files to expected dir
ln -n $RPC_SOURCE_PATH/journal/* $RPC_SOURCE_PATH/
ln -n $RPC_SOURCE_PATH/inner_command/* $RPC_SOURCE_PATH/
ln -n $RPC_SOURCE_PATH/replication/* $RPC_SOURCE_PATH/
ln -n $RPC_SOURCE_PATH/control_api/* $RPC_SOURCE_PATH/

#auto generate Makefiles
touch NEWS README AUTHORS ChangeLog
aclocal
libtoolize
libtoolize
autoheader
autoconf
automake -a
automake -a
automake
#compile...
cd ${CUR_PATH}
mkdir -p build
chmod +x ${CUR_PATH}/configure
if [ 2 -eq $# ] && [ x"-debug" = x$2 ]
then
    ${CUR_PATH}/configure --prefix=${CUR_PATH} CXXFLAGS='-g2 -O0 -w' CFLAGS='-g2 -O0 -w'
else
    ${CUR_PATH}/configure --prefix=${CUR_PATH}
fi

make clean
make -j 8
#copy program to bin
#cp ${CUR_PATH}/src/rpc_server ${CUR_PATH}/bin

elif [ "$1"a = "clean"a ]
then
make clean
#delete temporary building files
rm -rf aclocal.m4 config.guess config.log configure depcomp \
       install-sh ltmain.sh NEWS AUTHORS ChangeLog config.h \
       config.status m4 Makefile.in README stamp-h1 autom4te.cache  \
       compile config.h.in  config.sub COPYING INSTALL libtool Makefile missing

rm -rf build/
rm -rf ${RPC_SOURCE_PATH}/*.h ${RPC_SOURCE_PATH}/*.cc
rm -rf $RPC_SOURCE_PATH/inner_command
rm -rf $RPC_SOURCE_PATH/journal
rm -rf $RPC_SOURCE_PATH/control_api
rm -rf $RPC_SOURCE_PATH/replication
find . -name Makefile | xargs rm -f
find . -name Makefile.in | xargs rm -f
find . -name .deps | xargs rm -rf
else
echo ${USAGE}
fi
