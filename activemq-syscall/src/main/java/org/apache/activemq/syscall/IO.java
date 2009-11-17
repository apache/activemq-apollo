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

import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniField;
import org.fusesource.hawtjni.runtime.JniMethod;

import static org.fusesource.hawtjni.runtime.MethodFlag.*;

import static org.fusesource.hawtjni.runtime.FieldFlag.CONSTANT;

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
    @JniField(flags={CONSTANT}, conditional="#ifdef O_NONBLOCK")
    public static int O_NONBLOCK;
    @JniField(flags={CONSTANT}, conditional="#ifdef O_ASYNC")
    public static int O_ASYNC;
    
    @JniField(flags={CONSTANT}, conditional="#ifdef O_SHLOCK")
    public static int O_SHLOCK;
    @JniField(flags={CONSTANT}, conditional="#ifdef O_EXLOCK")
    public static int O_EXLOCK;
    @JniField(flags={CONSTANT}, conditional="#ifdef O_NOFOLLOW")
    public static int O_NOFOLLOW;
    @JniField(flags={CONSTANT}, conditional="#ifdef O_SYMLINK")
    public static int O_SYMLINK;
    @JniField(flags={CONSTANT}, conditional="#ifdef O_EVTONLY")
    public static int O_EVTONLY;
    
    // Mode Constants
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IRWXU")
    public static int S_IRWXU;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IRUSR")
    public static int S_IRUSR;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IWUSR")
    public static int S_IWUSR;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IXUSR")
    public static int S_IXUSR;            
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IRWXG")
    public static int S_IRWXG;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IRGRP")
    public static int S_IRGRP;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IWGRP")
    public static int S_IWGRP;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IXGRP")
    public static int S_IXGRP;            
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IRWXO")
    public static int S_IRWXO;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IROTH")
    public static int S_IROTH;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IWOTH")
    public static int S_IWOTH;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_IXOTH")
    public static int S_IXOTH;            
    @JniField(flags={CONSTANT}, conditional="#ifdef S_ISUID")
    public static int S_ISUID;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_ISGID")
    public static int S_ISGID;
    @JniField(flags={CONSTANT}, conditional="#ifdef S_ISVTX")
    public static int S_ISVTX;
    
    @JniField(flags={CONSTANT}, conditional="#ifdef F_DUPFD")
    public static int F_DUPFD;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_GETFD")
    public static int F_GETFD;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_SETFD")
    public static int F_SETFD;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_GETFL")
    public static int F_GETFL;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_SETFL")
    public static int F_SETFL;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_GETOWN")
    public static int F_GETOWN;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_SETOWN")
    public static int F_SETOWN;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_GETLK")
    public static int F_GETLK;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_SETLK")
    public static int F_SETLK;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_SETLKW")
    public static int F_SETLKW;

    @JniField(flags={CONSTANT}, conditional="#ifdef F_GETPATH")
    public static int F_GETPATH;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_PREALLOCATE")
    public static int F_PREALLOCATE;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_SETSIZE")
    public static int F_SETSIZE;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_RDADVISE")
    public static int F_RDADVISE;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_RDAHEAD")
    public static int F_RDAHEAD;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_READBOOTSTRAP")
    public static int F_READBOOTSTRAP;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_WRITEBOOTSTRAP")
    public static int F_WRITEBOOTSTRAP;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_NOCACHE")
    public static int F_NOCACHE;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_LOG2PHYS")
    public static int F_LOG2PHYS;
    @JniField(flags={CONSTANT}, conditional="#ifdef F_FULLFSYNC")
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

    /**
     * <code><pre>
     * int fcntl(int fd, int cmd, ...);
     * </pre></code>
     */
    public static final native int fcntl(int fd, int cmd);
        
    

}
