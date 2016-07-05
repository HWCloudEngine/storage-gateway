noinst_PROGRAMS=writer_client
AM_CPPFLAGS=-I$(top_srcdir)/src/include -I$(top_srcdir)/include/rpc -std=c++11
writer_client_SOURCES=writer_client.cc
writer_client_LDADD=$(top_srcdir)/src/rpc/librpc.la 
writer_client_LDFLAGS=-D_GNU_SOURCE
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++
SUBDIRS=../src/rpc
