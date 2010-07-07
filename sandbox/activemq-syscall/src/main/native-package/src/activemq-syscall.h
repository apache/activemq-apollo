#ifndef INCLUDED_ACTIVEMQ_SYSCALL_H
#define INCLUDED_ACTIVEMQ_SYSCALL_H

#ifdef HAVE_CONFIG_H
  /* configure based build.. we will use what it discovered about the platform */
  #include "config.h"
#elif _WINDOWS
  /* Windows based build */
  #define HAVE_STDLIB_H 1
  #define HAVE_STRINGS_H 1
  #define HAVE_IO_H 1
  #define bzero(ptr, len) memset(ptr, 0, len)
  #define open _open
  #define close _close 
  #define fcntl _fcntl
#endif

/* lets make sure we get the thread safe versions of the APIs */
#ifndef _REENTRANT
#define _REENTRANT
#endif

/* To get the linux posix extensions, consider moving
   this into the autoconf generation */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifdef HAVE_SYS_TYPES_H
  #include <sys/types.h>
#endif

#ifdef HAVE_SYS_UIO_H
  #include <sys/uio.h>
#endif

#ifdef HAVE_UNISTD_H
  #include <unistd.h>
#endif

#ifdef HAVE_STDLIB_H
  #include <stdlib.h>
#endif

#ifdef HAVE_STRINGS_H
  #include <string.h>
#endif

#ifdef HAVE_LIBAIO_H
  #include <libaio.h>
#endif

#ifdef HAVE_AIO_H
  #include <aio.h>
#endif

#ifdef HAVE_SYS_ERRNO_H
  #include <sys/errno.h>
#endif

#ifdef HAVE_SYS_STAT_H
  #include <sys/stat.h>
#endif

#ifdef HAVE_IO_H
  #include <io.h>
#endif

#ifdef HAVE_STDDEF_H
  #include <stddef.h>
#endif


#include <fcntl.h>

#define add(value1, value2) ((value1)+value2)

#endif /* INCLUDED_ACTIVEMQ_SYSCALL_H */
