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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.activemq.syscall.jni.IO;

import static org.apache.activemq.syscall.jni.CLibrary.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class FileDescriptor {

    private final int fd;
    boolean opened;

    public FileDescriptor(int fd) {
        this.fd = fd;
    }

    public static FileDescriptor open(String path, int oflags, int mode) throws IOException {
        int fd = IO.open(path, oflags, mode);
        if( fd== -1 ) {
            throw new IOException(string(strerror(errno())));
        }
        FileDescriptor rc = new FileDescriptor(fd);
        rc.opened = true;
        return rc;
    }
    
    public int dispose() {
        if(closeCheck()) {
            return IO.close(fd);
        }
        return 0;
    }

    public void close() throws IOException {
        if( dispose() == -1 ) {
            throw new IOException(string(strerror(errno())));
        }
    }

    private boolean closeCheck() {
        if( opened ) {
            opened=false;
            return true;
        }
        return false;
    }
    
    int getFD() {
        return fd;
    }

    public void write(NativeAllocation writeBuffer, Callable<Integer> callback) {
        return;
    }
    
}
