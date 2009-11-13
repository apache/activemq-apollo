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

import org.fusesource.hawtjni.runtime.JniArg;
import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniMethod;
import org.fusesource.hawtjni.runtime.Library;

import static org.fusesource.hawtjni.runtime.ArgFlag.*;
import static org.fusesource.hawtjni.runtime.Pointer.*;
/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass
public class CLibrary {

    final public static Library LIBRARY = new Library("activemq-syscall", CLibrary.class);
    static {
        LIBRARY.load();
    }
    
    ///////////////////////////////////////////////////////////////////
    //
    // String related methods 
    //
    ///////////////////////////////////////////////////////////////////
    
    public static final native int strlen(
            @JniArg(cast="char *")long s);

    ///////////////////////////////////////////////////////////////////
    //
    // Memory management related methods 
    //
    ///////////////////////////////////////////////////////////////////
    
    /**
     * <code><pre>
     * void * calloc(size_t count, size_t size);
     * </pre></code>
     */
    @JniMethod(cast="void *")
    public static final native long calloc(
            @JniArg(cast="size_t") long count,
            @JniArg(cast="size_t") long size);

    /**
     * <code><pre>
     * void * malloc(size_t len);
     * </pre></code>
     */
    @JniMethod(cast="void *")
    public static final native long malloc(
            @JniArg(cast="size_t") long size);
    
    /**
     * <code><pre>
     * void * memset(void *ptr, int c, size_t len);
     * </pre></code>
     */
    @JniMethod(cast="void *")
    public static final native long memset (
            @JniArg(cast="void *") long buffer, 
            int c, 
            @JniArg(cast="size_t") long num);
    
    /**
     * <code><pre>
     * void bzero(void *ptr, size_t len)
     * </pre></code>
     */
    public static final native void bzero(
            @JniArg(cast = "void *") long ptr, 
            long len);
    
    /**
     * <code><pre>
     * void free(void *ptr);
     * </pre></code>
     */
    public static final native void free(
            @JniArg(cast="void *") long ptr);
    
    ///////////////////////////////////////////////////////////////////
    //
    // Type conversion related methods.
    //
    ///////////////////////////////////////////////////////////////////
    
    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) char[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  short[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  int[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}, pointer=FALSE) long[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) float[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) double[] src, 
            @JniArg(cast="size_t") long size);

    
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) char[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) short[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) int[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}, pointer=FALSE) long[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) float[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) double[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  char[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) int[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src, 
            @JniArg(cast="size_t") long size);

}
