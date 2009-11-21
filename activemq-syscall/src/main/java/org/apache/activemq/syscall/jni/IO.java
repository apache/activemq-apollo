/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.syscall.jni;

import org.fusesource.hawtjni.runtime.ClassFlag;
import org.fusesource.hawtjni.runtime.FieldFlag;
import org.fusesource.hawtjni.runtime.JniArg;
import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniField;
import org.fusesource.hawtjni.runtime.JniMethod;

import static org.fusesource.hawtjni.runtime.ArgFlag.*;
import static org.fusesource.hawtjni.runtime.FieldFlag.*;
import static org.fusesource.hawtjni.runtime.MethodFlag.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass
public class IO {

    @JniMethod(flags={CONSTANT_INITIALIZER})
    private static final native void init();
    static {
        CLibrary.LIBRARY.load();
        init();
    }

    //////////////////////////////////////////////////////////////////
    //
    // Open mode constants.
    //
    //////////////////////////////////////////////////////////////////
    @JniField(flags={CONSTANT})
    public static int O_RDONLY;
    @JniField(flags={CONSTANT})
    public static int O_WRONLY;
    @JniField(flags={CONSTANT})
    public static int O_RDWR;
    @JniField(flags={CONSTANT})
    public static int O_APPEND;
    @JniField(flags={CONSTANT})
    public static int O_CREAT;
    @JniField(flags={CONSTANT})
    public static int O_TRUNC;
    @JniField(flags={CONSTANT})
    public static int O_EXCL;
    @JniField(flags={CONSTANT}, conditional="defined(O_NONBLOCK)")
    public static int O_NONBLOCK;
    @JniField(flags={CONSTANT}, conditional="defined(O_ASYNC)")
    public static int O_ASYNC;
    
    @JniField(flags={CONSTANT}, conditional="defined(O_SHLOCK)")
    public static int O_SHLOCK;
    @JniField(flags={CONSTANT}, conditional="defined(O_EXLOCK)")
    public static int O_EXLOCK;
    @JniField(flags={CONSTANT}, conditional="defined(O_NOFOLLOW)")
    public static int O_NOFOLLOW;
    @JniField(flags={CONSTANT}, conditional="defined(O_SYMLINK)")
    public static int O_SYMLINK;
    @JniField(flags={CONSTANT}, conditional="defined(O_EVTONLY)")
    public static int O_EVTONLY;
    
    @JniField(flags={CONSTANT}, conditional="defined(O_DIRECT)")
    public static int O_DIRECT;
    @JniField(flags={CONSTANT}, conditional="defined(O_CLOEXEC)")
    public static int O_CLOEXEC;
    @JniField(flags={CONSTANT}, conditional="defined(O_DIRECTORY)")
    public static int O_DIRECTORY;
    @JniField(flags={CONSTANT}, conditional="defined(O_LARGEFILE)")
    public static int O_LARGEFILE;
    @JniField(flags={CONSTANT}, conditional="defined(O_NOATIME)")
    public static int O_NOATIME;
    @JniField(flags={CONSTANT}, conditional="defined(O_NOCTTY)")
    public static int O_NOCTTY;
    @JniField(flags={CONSTANT}, conditional="defined(O_SYNC)")
    public static int O_SYNC;
    
    // Mode Constants
    @JniField(flags={CONSTANT}, conditional="defined(S_IRWXU)")
    public static int S_IRWXU;
    @JniField(flags={CONSTANT}, conditional="defined(S_IRUSR)")
    public static int S_IRUSR;
    @JniField(flags={CONSTANT}, conditional="defined(S_IWUSR)")
    public static int S_IWUSR;
    @JniField(flags={CONSTANT}, conditional="defined(S_IXUSR)")
    public static int S_IXUSR;            
    @JniField(flags={CONSTANT}, conditional="defined(S_IRWXG)")
    public static int S_IRWXG;
    @JniField(flags={CONSTANT}, conditional="defined(S_IRGRP)")
    public static int S_IRGRP;
    @JniField(flags={CONSTANT}, conditional="defined(S_IWGRP)")
    public static int S_IWGRP;
    @JniField(flags={CONSTANT}, conditional="defined(S_IXGRP)")
    public static int S_IXGRP;            
    @JniField(flags={CONSTANT}, conditional="defined(S_IRWXO)")
    public static int S_IRWXO;
    @JniField(flags={CONSTANT}, conditional="defined(S_IROTH)")
    public static int S_IROTH;
    @JniField(flags={CONSTANT}, conditional="defined(S_IWOTH)")
    public static int S_IWOTH;
    @JniField(flags={CONSTANT}, conditional="defined(S_IXOTH)")
    public static int S_IXOTH;            
    @JniField(flags={CONSTANT}, conditional="defined(S_ISUID)")
    public static int S_ISUID;
    @JniField(flags={CONSTANT}, conditional="defined(S_ISGID)")
    public static int S_ISGID;
    @JniField(flags={CONSTANT}, conditional="defined(S_ISVTX)")
    public static int S_ISVTX;
    
    @JniField(flags={CONSTANT}, conditional="defined(F_DUPFD)")
    public static int F_DUPFD;
    @JniField(flags={CONSTANT}, conditional="defined(F_GETFD)")
    public static int F_GETFD;
    @JniField(flags={CONSTANT}, conditional="defined(F_SETFD)")
    public static int F_SETFD;
    @JniField(flags={CONSTANT}, conditional="defined(F_GETFL)")
    public static int F_GETFL;
    @JniField(flags={CONSTANT}, conditional="defined(F_SETFL)")
    public static int F_SETFL;
    @JniField(flags={CONSTANT}, conditional="defined(F_GETOWN)")
    public static int F_GETOWN;
    @JniField(flags={CONSTANT}, conditional="defined(F_SETOWN)")
    public static int F_SETOWN;
    @JniField(flags={CONSTANT}, conditional="defined(F_GETLK)")
    public static int F_GETLK;
    @JniField(flags={CONSTANT}, conditional="defined(F_SETLK)")
    public static int F_SETLK;
    @JniField(flags={CONSTANT}, conditional="defined(F_SETLKW)")
    public static int F_SETLKW;

    @JniField(flags={CONSTANT}, conditional="defined(F_GETPATH)")
    public static int F_GETPATH;
    @JniField(flags={CONSTANT}, conditional="defined(F_PREALLOCATE)")
    public static int F_PREALLOCATE;
    @JniField(flags={CONSTANT}, conditional="defined(F_SETSIZE)")
    public static int F_SETSIZE;
    @JniField(flags={CONSTANT}, conditional="defined(F_RDADVISE)")
    public static int F_RDADVISE;
    @JniField(flags={CONSTANT}, conditional="defined(F_RDAHEAD)")
    public static int F_RDAHEAD;
    @JniField(flags={CONSTANT}, conditional="defined(F_READBOOTSTRAP)")
    public static int F_READBOOTSTRAP;
    @JniField(flags={CONSTANT}, conditional="defined(F_WRITEBOOTSTRAP)")
    public static int F_WRITEBOOTSTRAP;
    @JniField(flags={CONSTANT}, conditional="defined(F_NOCACHE)")
    public static int F_NOCACHE;
    @JniField(flags={CONSTANT}, conditional="defined(F_LOG2PHYS)")
    public static int F_LOG2PHYS;
    @JniField(flags={CONSTANT}, conditional="defined(F_FULLFSYNC)")
    public static int F_FULLFSYNC;
    
    ///////////////////////////////////////////////////////////////////
    //
    // IO related methods 
    //
    ///////////////////////////////////////////////////////////////////
    /**
     * <code><pre>
     * int open(const char *path, int oflags, ...);
     * </pre></code>
     */
    public static final native int open(String path, int oflags);
    
    /**
     * <code><pre>
     * int open(const char *path, int oflags, ...);
     * </pre></code>
     */
    public static final native int open(String path, int oflags, int mode);

    /**
     * <code><pre>
     * int close(int fd);
     * </pre></code>
     */
    public static final native int close(int fd);
    
    @JniField(flags={FieldFlag.CONSTANT}, conditional="defined(HAVE_FCNTL_FUNCTION)", accessor="1")
    public static boolean HAVE_FCNTL_FUNCTION;
    
    /**
     * <code><pre>
     * int fcntl(int fd, int cmd, ...);
     * </pre></code>
     */
    @JniMethod(conditional="defined(HAVE_FCNTL_FUNCTION)")
    public static final native int fcntl(int fd, int cmd);

    /**
     * <code><pre>
     * int fcntl(int fd, int cmd, ...);
     * </pre></code>
     */
    @JniMethod(conditional="defined(HAVE_FCNTL_FUNCTION)")
    public static final native int fcntl(int fd, int cmd, long arg);

    @JniField(flags={CONSTANT})
    public static int SEEK_SET;
    @JniField(flags={CONSTANT})
    public static int SEEK_CUR;
    @JniField(flags={CONSTANT})
    public static int SEEK_END;

    public static final native int fsync(int fd);

    @JniMethod(cast="off_t")
    public static final native long lseek(
            int fd, 
            @JniArg(cast="off_t") long buffer, 
            int whence);
    
    @JniMethod(cast="size_t")
    public static final native long read(
            int fd, 
            @JniArg(cast="void *") long buffer, 
            @JniArg(cast="size_t") long length);

    @JniMethod(cast="size_t")
    public static final native long write(
            int fd, 
            @JniArg(cast="const void *") long buffer, 
            @JniArg(cast="size_t") long length);

    @JniMethod(cast="size_t", conditional="defined(HAVE_PREAD_FUNCTION)")
    public static final native long pread(
            int fd, 
            @JniArg(cast="void *") long buffer, 
            @JniArg(cast="size_t") long length,
            @JniArg(cast="size_t") long offset);

    @JniMethod(cast="size_t", conditional="defined(HAVE_PREAD_FUNCTION)")
    public static final native long pwrite(
            int fd, 
            @JniArg(cast="const void *") long buffer, 
            @JniArg(cast="size_t") long length,
            @JniArg(cast="size_t") long offset);

    @JniMethod(cast="size_t", conditional="defined(HAVE_READV_FUNCTION)")
    public static final native long readv(
            int fd, 
            @JniArg(cast="const struct iovec *") long iov, 
            int count);

    @JniMethod(cast="size_t", conditional="defined(HAVE_READV_FUNCTION)")
    public static final native long writev(
            int fd, 
            @JniArg(cast="const struct iovec *") long iov, 
            int count);


    @JniClass(flags={ClassFlag.STRUCT}, conditional="defined(HAVE_READV_FUNCTION)")
    static public class iovec {

        static {
            CLibrary.LIBRARY.load();
            init();
        }

        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct iovec)", conditional="defined(HAVE_READV_FUNCTION)")
        public static int SIZEOF;
        
        @JniField(cast="char *")
        public long iov_base;  
        @JniField(cast="size_t")
        public long iov_len;
        
        @JniMethod(conditional="defined(HAVE_READV_FUNCTION)")
        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) iovec dest, 
                @JniArg(cast="const void *") long src, 
                @JniArg(cast="size_t") long size);
        
        @JniMethod(conditional="defined(HAVE_READV_FUNCTION)")
        public static final native void memmove (
                @JniArg(cast="void *") long dest, 
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) iovec src, 
                @JniArg(cast="size_t") long size);
        
        @JniMethod(cast="struct iovec *", accessor="add", conditional="defined(HAVE_READV_FUNCTION)")
        public static final native long iovec_add(
                @JniArg(cast="struct iovec *") long ptr, 
                long amount);        
    }      
    
}
