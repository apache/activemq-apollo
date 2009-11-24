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
import static org.fusesource.hawtjni.runtime.MethodFlag.*;

/**
 * Java binding to the libaio syscall facility on Linux.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass(conditional="defined(HAVE_LIBAIO_H)")
final public class LibAIO {
    static {
        CLibrary.LIBRARY.load();
        init();
    }
    
    @JniMethod(flags={CONSTANT_INITIALIZER})
    private static final native void init();
    
    @JniField(flags={FieldFlag.CONSTANT}, accessor="1")
    public static boolean SUPPORTED;
    
    
    @JniClass(flags={ClassFlag.STRUCT}, conditional="defined(HAVE_LIBAIO_H)")
    public static class iocb {
        static {
            CLibrary.LIBRARY.load();
            init();
        }
        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct iocb)")
        public static int SIZEOF;

        @JniField(flags={FieldFlag.CONSTANT}, accessor="offsetof(struct iocb, u)")
        public static int OFFSETOF_u;
        
        @JniField(cast="void *")
        public long data;
        @JniField(cast="unsigned")
        public int key;
        @JniField(cast="short")
        public short aio_lio_opcode; 
        @JniField(cast="short")
        public short aio_reqprio;
        @JniField(cast="int")
        public int aio_fildes;
        
        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) iocb dest, 
                @JniArg(cast="const void *") long src, 
                @JniArg(cast="size_t") long size);
        
        public static final native void memmove (
                @JniArg(cast="void *") long dest, 
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) iocb src, 
                @JniArg(cast="size_t") long size);        
    }

    @JniClass(flags={ClassFlag.STRUCT}, conditional="defined(HAVE_LIBAIO_H)")
    public static final class io_iocb_common {
        
        static {
            CLibrary.LIBRARY.load();
            init();
        }
        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct io_iocb_common)")
        public static int SIZEOF;
        
        @JniField(cast="void *")
        long buf;
        long nbytes;
        long offset;
        
        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) io_iocb_common dest, 
                @JniArg(cast="const void *") long src, 
                @JniArg(cast="size_t") long size);
        
        public static final native void memmove (
                @JniArg(cast="void *") long dest, 
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) io_iocb_common src, 
                @JniArg(cast="size_t") long size);        
    }
    
    @JniClass(flags={ClassFlag.STRUCT}, conditional="defined(HAVE_LIBAIO_H)")
    public static final class io_event {
        static {
            CLibrary.LIBRARY.load();
            init();
        }        
        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();
        
        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct io_event)")
        public static int SIZEOF;

        @JniField(cast="void *")
        public long data;
        @JniField(cast="struct iocb *")
        public long obj;
        public long res;
        public long res2;

        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) io_event dest, 
                @JniArg(cast="const void *") long src, 
                @JniArg(cast="size_t") long size);
        
        public static final native void memmove (
                @JniArg(cast="void *") long dest, 
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) io_event src, 
                @JniArg(cast="size_t") long size);        
    
    };

    ///////////////////////////////////////////////////////////////////
    // These are the actual syscalls 
    ///////////////////////////////////////////////////////////////////
    
    public static final native int io_setup(int nr_events, 
            @JniArg(cast="struct io_context **")long ctxp[]);
    
    public static final native int io_destroy(
            @JniArg(cast="struct io_context *")long ctx_id);
    
    public static final native int io_submit(
            @JniArg(cast="struct io_context *")long ctx_id, 
            long nr, 
            @JniArg(cast="struct iocb **")long iocbpp[]);
    
    public static final native int io_getevents(
            @JniArg(cast="struct io_context *")long ctx_id, 
            long min_nr, 
            long nr, 
            @JniArg(cast="struct io_event *")long events,
            @JniArg(cast="struct timespec *")long timeout
            );    

    public static final native int io_cancel(
            @JniArg(cast="struct io_context *")long ctx_id, 
            @JniArg(cast="struct iocb *")long iocbp,
            @JniArg(cast="struct io_event *")long resultp);


    ///////////////////////////////////////////////////////////////////
    // These are libaio helper functions 
    ///////////////////////////////////////////////////////////////////
    
    public static final native int io_queue_init(
            int maxevents, 
            @JniArg(cast="struct io_context **") long ctxp[]);
    public static final native int io_queue_release(
            @JniArg(cast="struct io_context *") long ctx);
    public static final native int io_queue_run(
            @JniArg(cast="struct io_context *") long ctx);

    public static final native void io_set_callback(
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="void *")long  cb);

    public static final native void io_prep_pread(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd, 
            @JniArg(cast="void *") long buf, 
            @JniArg(cast="size_t") long count, 
            long offset);

    public static final native void io_prep_pwrite(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd, 
            @JniArg(cast="void *") long buf, 
            @JniArg(cast="size_t") long count, 
            long offset);

    public static final native void io_prep_preadv(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd, 
            @JniArg(cast="const struct iovec *") long iov, 
            int iovcnt, 
            long offset);

    public static final native void io_prep_pwritev(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd, 
            @JniArg(cast="const struct iovec *") long iov, 
            int iovcnt, 
            long offset);

    public static final native int io_poll(
            @JniArg(cast="struct io_context *")long ctx, 
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="void *")long  cb, 
            int fd, 
            int events);

    public static final native void io_prep_fsync(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd);

    public static final native int io_fsync(
            @JniArg(cast="struct io_context *")long ctx, 
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="void *")long  cb, 
            int fd);

    public static final native void io_prep_fdsync(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd);

    public static final native int io_fdsync(
            @JniArg(cast="struct io_context *")long ctx, 
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="void *")long  cb, 
            int fd);

    public static final native void io_set_eventfd(
            @JniArg(cast="struct iocb *")long iocb, 
            int eventfd);
}
