#!/bin/sh
protoc -I ./protos --grpc_out=./source --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ./protos/writer.proto ./protos/consumer.proto
protoc -I ./protos --cpp_out=./source ./protos/writer.proto ./protos/consumer.proto ./protos/common.proto
