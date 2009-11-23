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
import java.util.Collection;

import org.apache.activemq.syscall.jni.PosixAIO;
import org.apache.activemq.syscall.jni.IO;
import org.apache.activemq.syscall.jni.PosixAIO.aiocb;
import org.apache.activemq.syscall.jni.IO.iovec;

import static org.apache.activemq.syscall.jni.PosixAIO.*;
import static org.apache.activemq.syscall.jni.CLibrary.*;
import static org.apache.activemq.syscall.jni.IO.*;
import static org.apache.activemq.syscall.jni.IO.iovec.*;

/**
 * Used to access a file descriptor.
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

    public static FileDescriptor open(String path, int oflags) throws IOException {
        return open(path, oflags, 0);
    }

    public static FileDescriptor open(File file, int oflags) throws IOException {
        return open(file.getPath(), oflags, 0);
    }

    public static FileDescriptor open(File file, int oflags, int mode) throws IOException {
        return open(file.getPath(), oflags, mode);
    }

    public static FileDescriptor open(String path, int oflags, int mode) throws IOException {
        int fd = IO.open(path, oflags, mode);
        if (fd == -1) {
            throw error();
        }
        FileDescriptor rc = new FileDescriptor(fd);
        rc.opened = true;
        return rc;
    }

    public int dispose() {
        if (closeCheck()) {
            return IO.close(fd);
        }
        return 0;
    }

    public void close() throws IOException {
        if (dispose() == -1) {
            throw error();
        }
    }

    private boolean closeCheck() {
        if (opened) {
            opened = false;
            return true;
        }
        return false;
    }

    int getFD() {
        return fd;
    }

    public long seek(long offset) throws IOException {
        return seek(offset, SEEK_SET);
    }
    
    public long seek(long offset, int whence) throws IOException {
        long rc = IO.lseek(fd, offset, whence);
        if (rc == -1) {
            throw error();
        }
        return rc;
    }

    public long write(NativeAllocation buffer) throws IOException {
        long rc = IO.write(fd, buffer.pointer(), buffer.length());
        if (rc == -1) {
            throw error();
        }
        return rc;
    }

    public long read(NativeAllocation buffer) throws IOException {
        long rc = IO.read(fd, buffer.pointer(), buffer.length());
        if (rc == -1) {
            throw error();
        }
        return rc;
    }

    public long write(long offset, NativeAllocation buffer) throws IOException {
        long rc = IO.pwrite(fd, buffer.pointer(), buffer.length(), offset);
        if (rc == -1) {
            throw error();
        }
        return rc;
    }

    public long read(long offset, NativeAllocation buffer) throws IOException {
        long rc = IO.pread(fd, buffer.pointer(), buffer.length(), offset);
        if (rc == -1) {
            throw error();
        }
        return rc;
    }

    public long write(Collection<NativeAllocation> buffers) throws IOException {
        long iovecp = malloc(iovec.SIZEOF * buffers.size());
        if (iovecp == NULL) {
            throw new OutOfMemoryError();
        }
        try {
            long cur = iovecp;
            iovec v = new iovec();
            for (NativeAllocation buffer : buffers) {
                v.iov_base = buffer.pointer();
                v.iov_len = buffer.length();
                memmove(cur, v, iovec.SIZEOF);
                cur = iovec_add(cur, 1);
            }
            long rc = IO.writev(fd, iovecp, buffers.size());
            if (rc == -1) {
                throw error();
            }
            return rc;
        } finally {
            free(iovecp);
        }
    }

    public long read(Collection<NativeAllocation> buffers) throws IOException {
        long iovecp = malloc(iovec.SIZEOF * buffers.size());
        if (iovecp == NULL) {
            throw new OutOfMemoryError();
        }
        try {
            long cur = iovecp;
            iovec v = new iovec();
            for (NativeAllocation buffer : buffers) {
                v.iov_base = buffer.pointer();
                v.iov_len = buffer.length();
                memmove(cur, v, iovec.SIZEOF);
                cur = iovec_add(cur, 1);
            }
            long rc = IO.readv(fd, iovecp, buffers.size());
            if (rc == -1) {
                throw error();
            }
            return rc;
        } finally {
            free(iovecp);
        }
    }

    public boolean isAsyncIOSupported() {
        return PosixAIO.SUPPORTED;
    }

    /**
     * Performs a non blocking write, the callback gets executed once the write
     * completes. The buffer should not be read until the operation completes.
     * 
     * @param buffer
     * @param callback
     */
    public void write(long offset, NativeAllocation buffer, Callback<Long> callback) throws IOException {
        long aiocbp = block(offset, buffer);
        int rc = aio_write(aiocbp);
        if (rc == -1) {
            free(aiocbp);
            throw error();
        }
        agent().watch(aiocbp, callback);
    }

    static private IOException error() {
        return new IOException(string(strerror(errno())));
    }

    /**
     * Performs a non blocking read, the callback gets executed once the read
     * completes. The buffer should not be modified until the operation
     * completes.
     * 
     * @param buffer
     * @param callback
     */
    public void read(long offset, NativeAllocation buffer, Callback<Long> callback) throws IOException {
        long aiocbp = block(offset, buffer);
        int rc = aio_read(aiocbp);
        if (rc == -1) {
            free(aiocbp);
            throw error();
        }
        agent().watch(aiocbp, callback);
    }

    public void sync() throws IOException {
        int rc = IO.fsync(fd);
        if( rc == -1 ) {
            throw error();
        }
    }

    public boolean isfullSyncSupported() {
        return F_FULLFSYNC != 0;
    }

    public void fullSync() throws IOException, UnsupportedOperationException {
        if (!isfullSyncSupported()) {
            throw new UnsupportedOperationException();
        }
        int rc = fcntl(fd, F_FULLFSYNC);
        if( rc == -1 ) {
            throw error();
        }
    }

    public boolean isDirectIOSupported() {
        if (!HAVE_FCNTL_FUNCTION)
            return false;
        if (F_NOCACHE != 0)
            return true;
        if (O_DIRECT != 0)
            return true;
        return false;
    }

    public void enableDirectIO() throws IOException, UnsupportedOperationException {
        if (F_NOCACHE != 0) {
            int rc = fcntl(fd, F_NOCACHE);
            if( rc == -1 ) {
                throw error();
            }
        } else if (O_DIRECT != 0) {
            int rc = fcntl(fd, F_GETFL);
            if( rc == -1 ) {
                throw error();
            }
            rc = fcntl(fd, F_SETFL, rc|O_DIRECT );
            if( rc == -1 ) {
                throw error();
            }
            
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * allocates an initialized aiocb structure on the heap using the given
     * parameters.
     */
    private long block(long offset, NativeAllocation buffer) throws OutOfMemoryError {
        aiocb cb = new aiocb();
        cb.aio_fildes = fd;
        cb.aio_offset = offset;
        cb.aio_buf = buffer.pointer();
        cb.aio_nbytes = buffer.length();

        long aiocbp = malloc(aiocb.SIZEOF);
        if (aiocbp == NULL) {
            throw new OutOfMemoryError();
        }
        aiocb.memmove(aiocbp, cb, aiocb.SIZEOF);
        return aiocbp;
    }

    /**
     * gets the poll agent that will be used to watch of completion of AIO
     * requets.
     * 
     * @return
     */
    private AioPollAgent agent() {
        if (aioPollAgent == null) {
            aioPollAgent = AioPollAgent.getMainPollAgent();
        }
        return aioPollAgent;
    }

    public void setAioPollAgent(AioPollAgent aioPollAgent) {
        this.aioPollAgent = aioPollAgent;
    }

}
