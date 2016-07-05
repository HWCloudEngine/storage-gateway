noinst_LTLIBRARIES=librpc.la
librpc_la_SOURCES=common.pb.cc consumer.grpc.pb.cc consumer.pb.cc  journal.pb.cc  writer.grpc.pb.cc  writer.pb.cc
AM_CPPFLAGS=-I$(top_srcdir)/src/include -I$(top_srcdir)/src/include/rpc -std=c++11
#AM_CXXFLAGS=-std=c++11
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++
