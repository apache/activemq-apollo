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

import org.apache.activemq.syscall.jni.CLibrary;

import static org.apache.activemq.syscall.jni.CLibrary.*;

/**
 * Wraps up a a native memory allocation in a a Java object
 * so that it can get garbage collected collected and so we can 
 * keep track of how big the allocation is.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class NativeAllocation {
	
    private long pointer;
    private long length;

    public NativeAllocation(long pointer, long length) {
        if( pointer==NULL ) {
            throw new OutOfMemoryError("jni failed to heap allocate: "+length);
        }
        this.pointer = pointer;
        this.length = length;
    }
    
    static public NativeAllocation allocate(String value) {
        return allocate(value.getBytes());
    }

    private static NativeAllocation allocate(byte[] value) {
        int size = value.length;
        NativeAllocation rc = allocate(size);
        memmove(rc.pointer(), value, size);
        return rc;
    }
    
    static public NativeAllocation allocate(long size) {
        return new NativeAllocation(calloc(size,1), size);
    }        
    
    synchronized public void free() {
        if( pointer!=NULL ) {
            CLibrary.free(pointer);
            pointer = 0;
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        free();
    }

    public long pointer() {
        return pointer;
    }
    
    public long offset(long offset) {
        return void_pointer_add(pointer, offset);
    }

    public long length() {
        return length;
    }

}