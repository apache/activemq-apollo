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

import java.io.File;
import java.io.IOException;

import org.apache.activemq.syscall.NativeAllocation;
import org.apache.activemq.syscall.jni.PosixAIO.aiocb;
import org.junit.Test;

import static org.apache.activemq.syscall.NativeAllocation.*;
import static org.apache.activemq.syscall.TestSupport.*;
import static org.apache.activemq.syscall.jni.PosixAIO.*;
import static org.apache.activemq.syscall.jni.CLibrary.*;
import static org.apache.activemq.syscall.jni.IO.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 */
public class PosixAIOTest {
    
    @Test
    public void write() throws IOException, InterruptedException {
    	assumeThat(PosixAIO.SUPPORTED, is(true));
    	 
        File file = dataFile(PosixAIOTest.class.getName()+".write.data");

        String expected = generateString(1024*4);
        NativeAllocation buffer = allocate(expected);

        long aiocbp = malloc(aiocb.SIZEOF);
        System.out.println("Allocated cb of size: "+aiocb.SIZEOF);

        try {
            // open the file...
            int oflags = O_NONBLOCK | O_CREAT | O_TRUNC| O_RDWR;
            int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
            int fd = open(file.getCanonicalPath(), oflags, mode);
            checkrc(fd);
            
            // Create a control block..
            // The where:
            aiocb cb = new aiocb();
            cb.aio_fildes = fd;
            cb.aio_offset = 0;
            // The what:
            cb.aio_buf = buffer.pointer();        
            cb.aio_nbytes = buffer.length();
            
            // Move the struct into the c heap.
            aiocb.memmove(aiocbp, cb, aiocb.SIZEOF);

            // enqueue the async write..
            checkrc(aio_write(aiocbp));
            
            long blocks[] = new long[]{aiocbp};
            
            // Wait for the IO to complete.
            long timeout = NULL; // To suspend forever.
            checkrc(aio_suspend(blocks, blocks.length, timeout));
            
            // Check to see if it completed.. it should 
            // since we previously suspended.
            int rc = aio_error(aiocbp);
            checkrc(rc);
            assertEquals(0, rc);

            // The full buffer should have been written.
            long count = aio_return(aiocbp);
            assertEquals(count, buffer.length());
            
            checkrc(close(fd));
            
        } finally {
            // Lets free up allocated memory..
            buffer.free();
            if( aiocbp!=NULL ) {
                free(aiocbp);
            }
        }
        
        assertEquals(expected, readFile(file));
    }


    private void checkrc(int rc) {
        if( rc==-1 ) {
            fail("IO failure: "+string(strerror(errno())));
        }
    }

}
