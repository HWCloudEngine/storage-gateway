noinst_LTLIBRARIES=librpc.la
librpc_la_SOURCES=common.pb.cc \
                  consumer.grpc.pb.cc consumer.pb.cc \
                  journal.pb.cc  \
                  writer.grpc.pb.cc  writer.pb.cc \
                  replicator.pb.cc replicator.grpc.pb.cc \
                  message.pb.cc  \
                  snapshot_control.grpc.pb.cc snapshot_control.pb.cc    \
                  snapshot_inner_control.grpc.pb.cc snapshot_inner_control.pb.cc  \
                  replicate_control.pb.cc replicate_control.grpc.pb.cc \
                  replicate_inner_control.pb.cc replicate_inner_control.grpc.pb.cc
AM_CPPFLAGS=-std=c++11
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++
