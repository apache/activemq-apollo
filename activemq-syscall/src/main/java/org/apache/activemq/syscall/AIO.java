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
package org.apache.activemq.syscall;

import org.fusesource.hawtjni.runtime.ClassFlag;
import org.fusesource.hawtjni.runtime.FieldFlag;
import org.fusesource.hawtjni.runtime.JniArg;
import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniField;
import org.fusesource.hawtjni.runtime.JniMethod;

import static org.fusesource.hawtjni.runtime.MethodFlag.*;

/**
 * The aio facility provides system calls for asynchronous I/O
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass
public class AIO extends CLibrary {

    @JniClass(flags={ClassFlag.STRUCT})
    static public class aiocb {
        static {
            LIBRARY.load();
            init();
        }
        
        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();
        
        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct aiocb)")
        public static int SIZEOF;
        
        int aio_fildes;
        @JniField(cast="void *")
        long aio_buf;
        @JniField(cast="size_t")
        long aio_nbytes;
        @JniField(cast="off_t")
        long aio_offset;
        // Don't need to access these right now:
        // int aio_reqprio;
        // struct sigevent aio_sigevent
        // int aio_lio_opcode;
        // int aio_flags;
    }
    
    @JniClass(flags={ClassFlag.STRUCT})
    static public class timespec {
        
        public static final native int timespec_sizeof ();
        public static final int SIZEOF = timespec_sizeof();
        
        @JniField(cast="time_t")
        long tv_sec;  
        @JniField(cast="long")
        long tv_nsec; 
    }    
    
    /**
     * <code><pre>
     * int aio_read(struct aiocb *aiocbp);
     * </pre></code>
     */
    public static final native int aio_read(
            @JniArg(cast="struct aiocb *")long aiocbp);

    /**
     * <code><pre>
     * int aio_cancel(int fd, struct aiocb *aiocbp);
     * </pre></code>
     */
    public static final native int aio_cancel(
            int fd, 
            @JniArg(cast="struct aiocb *")long aiocbp);
    
    /**
     * <code><pre>
     * int aio_error(const struct aiocb *aiocbp);
     * </pre></code>
     */
    public static final native int aio_error(
            @JniArg(cast="const struct aiocb *")long aiocbp);
    
    /**
     * <code><pre>
     * ssize_t aio_return(struct aiocb *aiocbp);
     * </pre></code>
     */
    public static final native long aio_return(
            @JniArg(cast="struct aiocb *")long aiocbp);

    /**
     * <code><pre>
     * int aio_suspend(const struct aiocb *const list[], int nent, const struct timespec *timeout);
     * </pre></code>
     */
    public static final native int aio_suspend(
            @JniArg(cast="const struct aiocb *const*")long[] list,
            int nent,
            @JniArg(cast="struct timespec *")long timeout);
    
}
