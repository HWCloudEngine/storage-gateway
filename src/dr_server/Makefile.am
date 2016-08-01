noinst_PROGRAMS=rpc_server
AM_CPPFLAGS=-I$(top_srcdir)/src -I$(top_srcdir)/src/include -std=c++11
ACLOCAL_AMFLAGS=-I m4
rpc_server_SOURCES=rpc_server.cc consumer_service.cc writer_service.cc ceph_s3_meta.cc
rpc_server_LDADD=${top_srcdir}/src/rpc/librpc.la
rpc_server_LDFLAGS=-D_GNU_SOURCE
DEFS+=-D_GNU_SOURCE
LDFLAGS="-L$(top_srcdir)/lib"
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++ -ls3
SUBDIRS=../rpc
