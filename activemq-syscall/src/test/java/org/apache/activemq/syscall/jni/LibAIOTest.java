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
import org.apache.activemq.syscall.jni.LibAIO.io_event;
import org.apache.activemq.syscall.jni.LibAIO.iocb;
import org.junit.Test;

import static org.apache.activemq.syscall.NativeAllocation.*;
import static org.apache.activemq.syscall.TestSupport.*;
import static org.apache.activemq.syscall.jni.CLibrary.*;
import static org.apache.activemq.syscall.jni.IO.*;
import static org.apache.activemq.syscall.jni.LibAIO.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 */
public class LibAIOTest {
    
    @Test
    public void write() throws IOException, InterruptedException {
    	assumeThat(LibAIO.SUPPORTED, is(true));
    	 
        File file = dataFile(LibAIOTest.class.getName()+".write.data");

        String expected = generateString(1024*4);
        NativeAllocation buffer = allocate(expected);

        long iocbp = malloc(iocb.SIZEOF);
        long io_eventp = malloc(io_event.SIZEOF);

        long ctx_ida[] = new long[1];
        int rc = io_setup(10, ctx_ida);
        assertEquals(0, rc);
        
        long ctx_id = ctx_ida[0];

        try {
            // open the file...
            int oflags = O_NONBLOCK | O_CREAT | O_TRUNC| O_RDWR;
            int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
            int fd = open(file.getCanonicalPath(), oflags, mode);
            checkrc(fd);
            
            // enqueue the async write..
            io_prep_pwrite(iocbp, fd, buffer.pointer(), buffer.length(), 0);
            rc = io_submit(ctx_id, 1, new long[]{iocbp});
            assertEquals(1, rc);

            // Wait for the IO to complete.
            rc = io_getevents(ctx_id, 1, 1, io_eventp, NULL);
            assertEquals(1, rc);
            
            // Get the event..
            // The full buffer should have been written.
            io_event event = new io_event();                        
            io_event.memmove(event, io_eventp, io_event.SIZEOF);
            
            assertEquals(0, event.res2);
            assertEquals(buffer.length(), event.res);
            assertEquals(event.obj, iocbp);
            
            checkrc(close(fd));
            
        } finally {
            // Lets free up allocated memory..
            io_destroy(ctx_id);            
            buffer.free();
            if( iocbp!=NULL ) {
                free(iocbp);
            }
            if( io_eventp!=NULL ) { 
                free(io_eventp);
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
