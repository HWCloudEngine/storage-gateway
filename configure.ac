#                                -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(rpc_server, 1.0, huawei@huawei.com)
# Checks for header files.
AM_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE
AC_CONFIG_MACRO_DIR([m4])
# Checks for programs.
#AC_PROG_CC
AC_LANG([C++])
AC_PROG_CXX
# Checks for libraries.
# FIXME: Replace `main' with a function in `-lpthread':
AC_CHECK_LIB([pthread],[pthread_rwlock_init])
# Checks for library functions.
#AC_CHECK_LIB([protobuf],[]) 
#AC_CHECK_LIB([dl],[])
#AC_CHECK_LIB([grpc],[])
#AC_CHECK_LIB([grpc++],[])
LT_INIT
AC_OUTPUT([Makefile
        src/log/Makefile
        src/rpc/Makefile
        src/sg_server/Makefile
        src/sg_client/Makefile
        src/test/Makefile
    ])
