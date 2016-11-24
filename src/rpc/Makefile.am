noinst_LTLIBRARIES=librpc.la
librpc_la_SOURCES=common.pb.cc \
                  consumer.grpc.pb.cc consumer.pb.cc \
                  journal.pb.cc  \
                  writer.grpc.pb.cc  writer.pb.cc \
<<<<<<< f9b4ef4a93855d453bb6483d14455aabbac005ee
                  message.pb.cc \
				  replicator.pb.cc replicator.grpc.pb.cc
=======
                  message.pb.cc  \
                  control.grpc.pb.cc control.pb.cc    \
                  snapshot.grpc.pb.cc snapshot.pb.cc 

>>>>>>> snapshot implement
AM_CPPFLAGS=-std=c++11
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++
