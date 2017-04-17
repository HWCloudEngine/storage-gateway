#!/bin/bash
#has_tool=`pip list | grep grpcio-tools`
build_home=${build_home:-$(cd $(dirname $0);pwd)}
echo "build_home:$build_home"
proto_path=../../src/rpc/protos

if [ 0 -eq $# ] || [ "$1"a = "make"a ]
then
    apt-get install python-pip
    has_tool=`pip list | grep grpcio-tools`
    if [[ $has_tool != grpcio-tools* ]] ;then
        pip install grpcio-tools
    fi
    #generate grpc python source files from protos
    python -m grpc.tools.protoc -I${proto_path} --python_out=. \
        --grpc_python_out=. ${proto_path}/control_api/snapshot_control.proto \
        ${proto_path}/control_api/replicate_control.proto \
        ${proto_path}/control_api/volume_control.proto \
        ${proto_path}/control_api/backup_control.proto \
        ${proto_path}/journal/journal.proto \
        ${proto_path}/common.proto \
        ${proto_path}/snapshot.proto \
        ${proto_path}/replicate.proto \
        ${proto_path}/volume.proto \
        ${proto_path}/backup.proto

    cd ${build_home}/control_api/
    echo "" > __init__.py
    cd ${build_home}/journal/
    echo "" > __init__.py

    cd ${build_home}
    mv *_pb* ${build_home}/control_api
    mv -f ${build_home}/journal ${build_home}/control_api
    mv -f ${build_home}/control_api ${build_home}/sg_control

    # install
    pip install setuptools
    pip install pbr
    cd ${build_home}
    python setup.py install

elif [ "$1"a = "clean"a ]
then
    rm *_pb2.py -rf
    rm *_pb2_grpc.py -rf
    rm journal/ -rf
    rm control_api/ -rf
    rm *.pyc -rf
fi
