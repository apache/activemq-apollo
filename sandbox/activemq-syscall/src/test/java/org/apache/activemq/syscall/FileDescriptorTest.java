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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.syscall.jni.PosixAIO;
import org.junit.Test;

import static org.apache.activemq.syscall.NativeAllocation.*;
import static org.apache.activemq.syscall.TestSupport.*;    
import static org.apache.activemq.syscall.jni.IO.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class FileDescriptorTest {
    
    @Test
    public void writeWithACallback() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        assumeThat(PosixAIO.SUPPORTED, is(true));
        
        String expected = generateString(1024*4);
        NativeAllocation buffer = allocate(expected);

        File file = dataFile(FileDescriptorTest.class.getName()+".writeWithACallback.data");
        
        int oflags = O_NONBLOCK | O_CREAT | O_TRUNC | O_WRONLY;
        int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
        FileDescriptor fd = FileDescriptor.open(file, oflags, mode);
        
        try {
            FutureCallback<Long> future = new FutureCallback<Long>();
            fd.write(0, buffer, future);
            long count = future.get(1, TimeUnit.SECONDS);
            
            assertEquals(count, buffer.length());
        } finally {
            fd.dispose();
        }
        
        assertEquals(expected, readFile(file));
        buffer.free();
    }
    
    
    
    @Test
    public void readWithACallback() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        assumeThat(PosixAIO.SUPPORTED, is(true));
        
        String expected = generateString(1024*4);
        
        File file = dataFile(FileDescriptorTest.class.getName()+".writeWithACallback.data");
        writeFile(file, expected);

        NativeAllocation buffer = allocate(expected.length());

        int oflags = O_RDONLY;
        FileDescriptor fd = FileDescriptor.open(file, oflags);
        
        try {
            FutureCallback<Long> future = new FutureCallback<Long>();
            fd.read(0, buffer, future);
            long count = future.get(1, TimeUnit.SECONDS);
            assertEquals(count, buffer.length());
        } finally {
            fd.dispose();
        }

        assertEquals(expected, buffer.string() );
        buffer.free();
    }
    
    @Test
    public void read() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        assumeThat(PosixAIO.SUPPORTED, is(true));
        
        String expected = "Hello World";
        
        File file = dataFile(FileDescriptorTest.class.getName()+".writeWithACallback.data");
        writeFile(file, expected);

        NativeAllocation buffer = allocate(6);

        int oflags = O_RDONLY;
        FileDescriptor fd = FileDescriptor.open(file, oflags);
        try {
        
            long size = fd.read(buffer);
            assertEquals(6, size);
            assertEquals(expected.substring(0, 6), buffer.string());
            
            size = fd.read(buffer);
            assertEquals(expected.length()-6, size);
            assertEquals(expected.substring(6), buffer.view(0, size).string());
        
        } finally {
            fd.dispose();
        }
        buffer.free();
    }    

}
