# ---------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ---------------------------------------------------------------------------

## --------------------------------
## Initialization macros.
## --------------------------------
AC_PREREQ([2.61])
AC_INIT([activemq-syscall], [6.0-SNAPSHOT])
AC_CONFIG_AUX_DIR([autotools])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/activemq-syscall.c])
AC_CONFIG_HEADERS([src/config.h])
AC_CANONICAL_HOST
AC_CANONICAL_SYSTEM

## -----------------------------------------------
## Application Checks
## -----------------------------------------------
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LIBTOOL([disable-static])

## -----------------------------------------------
## API Checks
## -----------------------------------------------

WITH_OSX_UNIVERSAL
WITH_JNI_JDK

# Look for the Posix AIO api..
AC_CHECK_HEADER([aio.h],[
  for lib in [aio c rt] ; do
    AC_CHECK_LIB([$lib], [aio_read],[
      LDFLAGS="$LDFLAGS -l$lib"
      AC_DEFINE([HAVE_AIO_H], [1], [Define to 1 if you have the <aio.h> header file.])
      break
    ])  
  done
])

# Look for the libaio api..
AC_CHECK_HEADER([libaio.h],[
  AC_CHECK_LIB([aio], [io_queue_run],[
    LDFLAGS="$LDFLAGS -laio"
    AC_DEFINE([HAVE_LIBAIO_H], [1], [Define to 1 if you have the <libaio.h> header file.])
    break
  ])  
])

AC_CHECK_LIB([c], [fcntl],[AC_DEFINE([HAVE_FCNTL_FUNCTION], [1], [Define to 1 if you have the fcntl function.])])
AC_CHECK_LIB([c], [posix_memalign],[AC_DEFINE([HAVE_POSIX_MEMALIGN_FUNCTION], [1], [Define to 1 if you have the posix_memalign function.])])
AC_CHECK_LIB([c], [pread],[AC_DEFINE([HAVE_PREAD_FUNCTION], [1], [Define to 1 if you have the pread function.])])
AC_CHECK_LIB([c], [readv],[AC_DEFINE([HAVE_READV_FUNCTION], [1], [Define to 1 if you have the readv function.])])
              
AC_CHECK_HEADER([sys/errno.h],[AC_DEFINE([HAVE_SYS_ERRNO_H], [1], [Define to 1 if you have the <sys/errno.h> header file.])])
AC_CHECK_HEADER([sys/stat.h],[AC_DEFINE([HAVE_SYS_STAT_H], [1], [Define to 1 if you have the <sys/stat.h> header file.])])
AC_CHECK_HEADER([sys/types.h],[AC_DEFINE([HAVE_SYS_TYPES_H], [1], [Define to 1 if you have the <sys/types.h> header file.])])
AC_CHECK_HEADER([sys/uio.h],[AC_DEFINE([HAVE_SYS_UIO_H], [1], [Define to 1 if you have the <sys/uio.h> header file.])])
AC_CHECK_HEADER([stddef.h],[AC_DEFINE([HAVE_STDDEF_H], [1], [Define to 1 if you have the <stddef.h> header file.])])

CFLAGS="$CFLAGS $JNI_EXTRA_CFLAGS"
LDFLAGS="$LDFLAGS $JNI_EXTRA_LDFLAGS -release $PACKAGE_VERSION"

AC_SUBST(CFLAGS)
AC_SUBST(LDFLAGS)

## -----------------------------------------------------
## Generate the files
## -----------------------------------------------------
AM_INIT_AUTOMAKE([no-dependencies -Wall -Werror foreign])
AC_CONFIG_FILES([Makefile])
AC_OUTPUT

echo "
  ($PACKAGE_NAME) version $PACKAGE_VERSION
  Prefix.........: $prefix
  C Compiler.....: $CC $CFLAGS
  Linker.........: $LD $LDFLAGS $LIBS
"