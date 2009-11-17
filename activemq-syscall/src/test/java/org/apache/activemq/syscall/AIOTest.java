package org.apache.activemq.syscall;

import static org.apache.activemq.syscall.AIO.*;
import static org.apache.activemq.syscall.AIOTest.NativeBuffer.*;
import static org.apache.activemq.syscall.CLibrary.*;
import static org.apache.activemq.syscall.IO.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.activemq.syscall.AIO.aiocb;
import org.junit.Test;

public class AIOTest {
    
    public static final class NativeBuffer {
    	
        long pointer;
        int offset;
        long length;


        public NativeBuffer(long pointer, int offset, long length) {
            if( pointer==NULL ) {
                throw new OutOfMemoryError("jni failed to heap allocate: "+length);
            }
            this.pointer = pointer;
            this.offset = offset;
            this.length = length;
        }
        
        static public NativeBuffer nativeBuffer(String value) {
            byte[] bytes = value.getBytes();
            long pointer = malloc(bytes.length);
            NativeBuffer rc = new NativeBuffer(pointer, 0, bytes.length);
            memmove(pointer, bytes, bytes.length);
            return rc;
        }
        
        static public NativeBuffer nativeBuffer(long size) {
            return new NativeBuffer(calloc(size,1), 0, size);
        }        
        
        public void free() {
            if( pointer!=NULL ) {
                CLibrary.free(pointer);
                pointer = 0;
            }
        }
        
    }
    
    @Test
    public void writeThenRead() throws IOException, InterruptedException {
    	assumeThat(AIO.SUPPORTED, is(true));
    	 
        File file = new File("target/test-data/test.data");
        file.getParentFile().mkdirs();

        // Setup a buffer holds the data that we will be writing..
        StringBuffer sb = new StringBuffer();
        for( int i=0; i < 1024*4; i++ ) {
            sb.append((char)('a'+(i%26)));
        }
                
        String expected = sb.toString();
        NativeBuffer writeBuffer = nativeBuffer(expected);

        long aiocbp = malloc(aiocb.SIZEOF);
        System.out.println("Allocated cb of size: "+aiocb.SIZEOF);

        try {
            // open the file...
            int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
            int fd = open(file.getCanonicalPath(), O_NONBLOCK | O_CREAT | O_TRUNC| O_RDWR, mode);
            checkrc(fd);
            
            // Create a control block..
            // The where:
            aiocb cb = new aiocb();
            cb.aio_fildes = fd;
            cb.aio_offset = 0;
            // The what:
            cb.aio_buf = writeBuffer.pointer;        
            cb.aio_nbytes = writeBuffer.length;
            
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
            assertEquals(count, writeBuffer.length);
            
            checkrc(close(fd));
            
        } finally {
            // Lets free up allocated memory..
            writeBuffer.free();
            if( aiocbp!=NULL ) {
                free(aiocbp);
            }
        }
        
        // Read the file in and verify the contents is what we expect 
        String actual = loadContent(file);
        assertEquals(expected, actual);
    }

    private String loadContent(File file) throws FileNotFoundException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileInputStream is = new FileInputStream(file);
        try {
            int c=0;
            while( (c=is.read())>=0 ) {
                baos.write(c);
            }
        } finally {
            is.close();
        }
        String actual = new String(baos.toByteArray());
        return actual;
    }

    private void storeContent(File file, String content) throws FileNotFoundException, IOException {
        FileOutputStream os = new FileOutputStream(file);
        try {
            os.write(content.getBytes());
        } finally {
            os.close();
        }
    }
    
    private void checkrc(int rc) throws IOException {
        if( rc==-1 ) {
            throw new IOException("IO failure: "+string(strerror(errno())));
        }
    }

    @Test
    public void testFree() {
        long ptr = malloc(100);
        free(ptr);
    }
    
}
