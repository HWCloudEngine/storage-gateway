#!/bin/bash
#has_tool=`pip list | grep grpcio-tools`
proto_path=../../src/rpc/protos
if [ 0 -eq $# ] || [ "$1"a = "make"a ]
then
    has_tool=`pip list | grep grpcio-tools`
    if [[ $has_tool != grpcio-tools* ]] ;then
        pip install grpcio-tools
    fi
    #generate grpc python source files from protos
    python -m grpc.tools.protoc -I${proto_path} --python_out=. \
        --grpc_python_out=. ${proto_path}/control.proto \
        ${proto_path}/replicate_control.proto \
        ${proto_path}/journal.proto
elif [ "$1"a = "clean"a ]
then
    rm *_pb2.py -rf
    rm *_pb2_grpc.py -rf
    rm *.pyc -rf
fi
