noinst_LTLIBRARIES=librpc.la
librpc_la_SOURCES=common.pb.cc   \
                  snapshot.pb.cc \
                  backup.pb.cc   \
                  volume.pb.cc \
                  replicate.pb.cc \
                  consumer.grpc.pb.cc consumer.pb.cc \
                  journal.pb.cc  \
                  writer.grpc.pb.cc  writer.pb.cc \
                  transfer.pb.cc transfer.grpc.pb.cc \
                  message.pb.cc  \
                  snapshot_control.grpc.pb.cc snapshot_control.pb.cc    \
                  snapshot_inner_control.grpc.pb.cc snapshot_inner_control.pb.cc  \
                  replicate_control.pb.cc replicate_control.grpc.pb.cc \
                  replicate_inner_control.pb.cc replicate_inner_control.grpc.pb.cc \
                  volume_control.pb.cc volume_control.grpc.pb.cc  \
                  volume_inner_control.pb.cc volume_inner_control.grpc.pb.cc  \
                  backup_control.pb.cc backup_control.grpc.pb.cc  \
                  backup_inner_control.pb.cc backup_inner_control.grpc.pb.cc \
                  lease.pb.cc lease.grpc.pb.cc

AM_CPPFLAGS=-std=c++11
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++
