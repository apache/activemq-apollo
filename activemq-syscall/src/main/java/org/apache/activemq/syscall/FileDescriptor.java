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

import java.io.File;
import java.io.IOException;

import org.apache.activemq.syscall.jni.IO;
import org.apache.activemq.syscall.jni.AIO.aiocb;

import static org.apache.activemq.syscall.jni.AIO.*;
import static org.apache.activemq.syscall.jni.CLibrary.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class FileDescriptor {

    private final int fd;
    boolean opened;
    private AioPollAgent aioPollAgent;

    public FileDescriptor(int fd) {
        this.fd = fd;
    }

    public static FileDescriptor open(File file, int oflags, int mode) throws IOException {
        return open(file.getPath(), oflags, mode); 
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

    /**
     * does an async write, the callback gets executed once the write completes.
     * 
     * @param buffer
     * @param callback
     */
    public void write(long offset, NativeAllocation buffer, Callback<Long> callback) throws IOException {
        
        aiocb cb = new aiocb();
        cb.aio_fildes = fd;
        cb.aio_offset = offset;
        cb.aio_buf = buffer.pointer();        
        cb.aio_nbytes = buffer.length();

        long aiocbp = malloc(aiocb.SIZEOF);
        if( aiocbp==NULL ) {
            throw new OutOfMemoryError();
        }
        aiocb.memmove(aiocbp, cb, aiocb.SIZEOF);
        aio_write(aiocbp);

        AioPollAgent agent = getAioPollAgent();
        agent.watch(aiocbp, callback);
        return;
    }
    
    private AioPollAgent getAioPollAgent() {
        if( aioPollAgent==null ) {
            aioPollAgent = AioPollAgent.getMainPollAgent();
        }
        return aioPollAgent;
    }

    public void setAioPollAgent(AioPollAgent aioPollAgent) {
        this.aioPollAgent = aioPollAgent;
    }

    /**
     * does an async read, the callback gets executed once the read completes.
     * 
     * @param buffer
     * @param callback
     */
    public void read(long offset, NativeAllocation buffer, Callback<Long> callback) throws IOException {
        

        return;
    }
    
    
    
}
