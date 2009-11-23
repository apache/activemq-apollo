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
    public static final class iocb {
        static {
            CLibrary.LIBRARY.load();
            init();
        }
        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct iocb)")
        public static int SIZEOF;
        
        @JniField(cast="void *")
        long data;
        @JniField(cast="unsigned")
        int key;
        @JniField(cast="short")
        short aio_lio_opcode; 
        @JniField(cast="short")
        short aio_reqprio;
        @JniField(cast="int")
        int aio_fildes;
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
        long data;
        @JniField(cast="struct iocb *")
        long obj;
        long res;
        long res2;
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
            @JniArg(cast="struct io_context **") long ctx);

    public static final native void io_set_callback(
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="io_callback_t")long  cb);

    public static final native void io_prep_pread(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd, 
            @JniArg(cast="void *") long buf, 
            @JniArg(cast="site_t") long count, 
            long offset);

    public static final native void io_prep_pwrite(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd, 
            @JniArg(cast="void *") long buf, 
            @JniArg(cast="site_t") long count, 
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
            @JniArg(cast="io_callback_t")long  cb, 
            int fd, 
            int events);

    public static final native void io_prep_fsync(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd);

    public static final native int io_fsync(
            @JniArg(cast="struct io_context *")long ctx, 
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="io_callback_t")long  cb, 
            int fd);

    public static final native void io_prep_fdsync(
            @JniArg(cast="struct iocb *")long iocb, 
            int fd);

    public static final native int io_fdsync(
            @JniArg(cast="struct io_context *")long ctx, 
            @JniArg(cast="struct iocb *")long iocb, 
            @JniArg(cast="io_callback_t")long  cb, 
            int fd);

    public static final native void io_set_eventfd(
            @JniArg(cast="struct iocb *")long iocb, 
            int eventfd);
}
