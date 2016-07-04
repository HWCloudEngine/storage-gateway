#                                -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(rpc_server, 1.0, huawei@huawei.com)
AC_CONFIG_SRCDIR([src/rpc_server.cc])
AM_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE

# Checks for programs.
#AC_PROG_CC
AC_LANG([C++])
AC_PROG_CXX
# Checks for libraries.
# FIXME: Replace `main' with a function in `-lpthread':
AC_CHECK_LIB([pthread],[pthread_rwlock_init])
#AC_CHECK_LIB([protobuf],[]) 
#AC_CHECK_LIB([dl],[])
#AC_CHECK_LIB([grpc],[])
#AC_CHECK_LIB([grpc++],[])
AC_PROG_RANLIB
LT_INIT
# Checks for header files.
# Checks for typedefs, structures, and compiler characteristics.
# Checks for library functions.
AC_OUTPUT([Makefile
		src/Makefile
		src/rpc/Makefile
		test/Makefile
		])
