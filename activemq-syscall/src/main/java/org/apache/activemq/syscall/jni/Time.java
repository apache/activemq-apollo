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
 * Time related functions and structures.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
// @JniClass(conditional="defined(HAVE_LIBAIO_H)")
public class Time {

    static {
        CLibrary.LIBRARY.load();
    }
    
    @JniClass(flags={ClassFlag.STRUCT})
    static public class timespec {

        static {
            CLibrary.LIBRARY.load();
            init();
        }

        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={FieldFlag.CONSTANT}, accessor="sizeof(struct timespec)")
        public static int SIZEOF;
        
        @JniField(cast="time_t")
        public long tv_sec;  
        @JniField(cast="long")
        public long tv_nsec;
        
        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) timespec dest, 
                @JniArg(cast="const void *") long src, 
                @JniArg(cast="size_t") long size);
        
        public static final native void memmove (
                @JniArg(cast="void *") long dest, 
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) timespec src, 
                @JniArg(cast="size_t") long size);
    }     

}