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

import org.fusesource.hawtjni.runtime.JniArg;
import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniField;
import org.fusesource.hawtjni.runtime.JniMethod;
import org.fusesource.hawtjni.runtime.Library;
import org.fusesource.hawtjni.runtime.MethodFlag;

import static org.fusesource.hawtjni.runtime.MethodFlag.*;

import static org.fusesource.hawtjni.runtime.FieldFlag.*;

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
        init();
    }
    
    @JniMethod(flags={CONSTANT_INITIALIZER})
    private static final native void init();

    final public static long NULL = 0;
    
    @JniField(flags={CONSTANT}, conditional="defined(ENOMEM)")
    public static int ENOMEM;

    @JniField(flags={CONSTANT}, conditional="defined(EINVAL)")
    public static int EINVAL;
    
    @JniMethod(flags={MethodFlag.CONSTANT_GETTER})
    public static final native int errno();

    @JniMethod(cast="char *")
    public static final native long strerror(int errnum);
    
    public static final native int strlen(
            @JniArg(cast="char *")long s);
    
    public static String string(long ptr) {
        if( ptr == NULL )
            return null;
        int length = strlen(ptr);
        byte[] data = new byte[length];
        memmove(data, ptr, length);
        return new String(data);
    }

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
     * void free(void *ptr);
     * </pre></code>
     */
    public static final native void free(
            @JniArg(cast="void *") long ptr);
    
    ///////////////////////////////////////////////////////////////////
    //
    // Generic void * helpers..
    //
    ///////////////////////////////////////////////////////////////////
    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);
    
    ///////////////////////////////////////////////////////////////////
    //
    // byte helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////
    
    @JniMethod(cast="jbyte *", accessor="add")
    public static final native long byte_pointer_add(
            @JniArg(cast="jbyte *") long ptr, 
            long amount);
    
    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);
    
    ///////////////////////////////////////////////////////////////////
    //
    // char helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////
    
    @JniMethod(cast="jchar *", accessor="add")
    public static final native long char_pointer_add(
            @JniArg(cast="jchar *") long ptr, 
            long amount);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) char[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) char[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);
    
    ///////////////////////////////////////////////////////////////////
    //
    // short helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////
    
    @JniMethod(cast="jshort *", accessor="add")
    public static final native long short_pointer_add(
            @JniArg(cast="jshort *") long ptr, 
            long amount);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  short[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) short[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    
    ///////////////////////////////////////////////////////////////////
    //
    // int helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////
    
    @JniMethod(cast="jint *", accessor="add")
    public static final native long int_pointer_add(
            @JniArg(cast="jint *") long ptr, 
            long amount);
    
    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  int[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) int[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    
    ///////////////////////////////////////////////////////////////////
    //
    // long helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////

    @JniMethod(cast="jlong *", accessor="add")
    public static final native long long_pointer_add(
            @JniArg(cast="jlong *") long ptr, 
            long amount);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}, pointer=FALSE) long[] src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}, pointer=FALSE) long[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);
    
    ///////////////////////////////////////////////////////////////////
    //
    // float helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////
    
    @JniMethod(cast="jfloat *", accessor="add")
    public static final native long float_pointer_add(
            @JniArg(cast="jfloat *") long ptr, 
            long amount);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) float[] src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) float[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);

    ///////////////////////////////////////////////////////////////////
    //
    // double helpers / converters.
    //
    ///////////////////////////////////////////////////////////////////
    
    @JniMethod(cast="jdouble *", accessor="add")
    public static final native long double_pointer_add(
            @JniArg(cast="jdouble *") long ptr, 
            long amount);

    public static final native void memmove (
            @JniArg(cast="void *") long dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) double[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) double[] dest, 
            @JniArg(cast="const void *") long src, 
            @JniArg(cast="size_t") long size);
    
    
    ///////////////////////////////////////////////////////////////////
    //
    // Common array type converters..
    //
    ///////////////////////////////////////////////////////////////////

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  char[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) char[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL})  byte[] src, 
            @JniArg(cast="size_t") long size);

    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) int[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) int[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) short[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) short[] src, 
            @JniArg(cast="size_t") long size);

    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) long[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src, 
            @JniArg(cast="size_t") long size);
    
    public static final native void memmove (
            @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest, 
            @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) long[] src, 
            @JniArg(cast="size_t") long size);
    
}
