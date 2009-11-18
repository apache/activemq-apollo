package org.apache.activemq.syscall.jni;

import org.apache.activemq.syscall.jni.CLibrary;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.activemq.syscall.jni.CLibrary.*;


public class CLibraryTest {
    
    @Test
    public void testMalloc() {
        long ptr = CLibrary.malloc(100);
        Assert.assertTrue(ptr!=0);
    }
    
    @Test
    public void testFree() {
        long ptr = malloc(100);
        free(ptr);
    }
    
}
