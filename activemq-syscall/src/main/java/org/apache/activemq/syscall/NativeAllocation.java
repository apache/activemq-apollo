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

import java.io.UnsupportedEncodingException;

import org.apache.activemq.syscall.jni.CLibrary;

import static org.apache.activemq.syscall.jni.CLibrary.*;
import static org.apache.activemq.syscall.jni.Posix.*;

/**
 * Wraps up a a native memory allocation in a a Java object
 * so that it can get garbage collected collected and so we can 
 * keep track of how big the allocation is.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class NativeAllocation {

    final private long pointer;
    final private long length;
    volatile byte allocated;

    public NativeAllocation(long pointer, long length) {
        if( pointer==NULL ) {
            throw new OutOfMemoryError("jni failed to heap allocate: "+length);
        }
        this.pointer = pointer;
        this.length = length;
    }
    
    public static NativeAllocation allocate(byte[] value) {
        int size = value.length;
        NativeAllocation rc = allocate(size, false);
        memmove(rc.pointer(), value, size);
        return rc;
    }
    
    static public NativeAllocation allocate(long size) {
        return allocate(size, true);
    }
    
    static public NativeAllocation allocate(long size, boolean zero) {
        long ptr;
        if( zero ) {
            ptr = calloc(size,1);
        } else {
            ptr = malloc(size);
        }
        NativeAllocation rc = new NativeAllocation(ptr, size);
        rc.allocated = 1;
        return rc;
    }        

    static public NativeAllocation allocate(long size, boolean zero, long alignment) {
        long ptrp[] = new long[1];
        int rc = posix_memalign(ptrp, alignment, size);
        if( rc != 0 ) {
            if( rc == EINVAL ) {
                throw new IllegalArgumentException("The alignment parameter is not a power of 2 and at least as large as sizeof(void *)");
            }
            throw new OutOfMemoryError();
        }
        if( zero ) {
            memset(ptrp[0], 0, size);
        }
        NativeAllocation na = new NativeAllocation(ptrp[0], size);
        na.allocated = 1;
        return na;
    }        
    
    public void free() {
        // This should be thread safe as long as the JVM continues
        // to do the unary decrement on a byte is atomic operation
        if( allocated==1 && (--allocated)==0 ) {
            CLibrary.free(pointer);
        }
    }

    /**
     * This finalize is here as a fail safe to fee up memory that was not freed
     * manually. 
     * 
     * @see java.lang.Object#finalize()
     */
    protected void finalize() throws Throwable {
        if( allocated==1 && (--allocated)==0 ) {
            assert warnAboutALeak();
            CLibrary.free(pointer);
        }
    }
    
    private boolean warnAboutALeak() {
        System.err.println(String.format("Warnning: memory leak avoided, a NativeAllocation was not freed: %d", pointer));
        return true;
    }

    public NativeAllocation view(long off, long len) {
        assert len >=0;
        assert off >=0;
        assert off+len <= length;
        long ptr = pointer;
        if( off > 0 ) {
            ptr = byte_pointer_add(ptr, off);
        }
        return new NativeAllocation(ptr, len);
    }    

    public long pointer() {
        return pointer;
    }
    
    public long offset(long offset) {
        return byte_pointer_add(pointer, offset);
    }

    public long length() {
        return length;
    }

    public byte[] bytes() {
        if( length > Integer.MAX_VALUE ) {
            throw new IndexOutOfBoundsException("The native allocation is to large to convert to a java byte array");
        }
        byte rc[] = new byte[(int) length];
        memmove(rc, pointer, length);
        return rc;
    }
    
    static public NativeAllocation allocate(String value) {
        return allocate(value.getBytes());
    }

    static public NativeAllocation allocate(String value, String encoding) throws UnsupportedEncodingException {
        return allocate(value.getBytes(encoding));
    }

    public String string() {
        return new String(bytes());
    }
    
    public String string(String encoding) throws UnsupportedEncodingException {
        return new String(bytes(), encoding);
    }

    public void set(byte[] data) {
        if( data.length > length ) {
            throw new IllegalArgumentException("data parameter is larger than the native allocation");
        }
        memmove(pointer, data, data.length);
    }

    public void get(byte[] data) {
        if( data.length < length ) {
            throw new IllegalArgumentException("data parameter is smaller than the native allocation");
        }
        memmove(data, pointer, length);
    }

}