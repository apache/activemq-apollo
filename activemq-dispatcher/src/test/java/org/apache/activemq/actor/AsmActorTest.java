package org.apache.activemq.actor;

import junit.framework.Assert;

import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;
import org.junit.Test;

import static junit.framework.Assert.*;

import static junit.framework.Assert.*;

import static junit.framework.Assert.*;

import static junit.framework.Assert.*;

import static junit.framework.Assert.*;

public class AsmActorTest {

    public static interface TestInterface {
        void strings(String value, String[] value2);
        void shorts(short value, short[] value2);
        String returnString();
    }
    
    static class TestMock implements TestInterface {
        public void shorts(short value, short[] value2) {
            fail();
        }
        public void strings(String value, String[] value2) {
            fail();
        }
        public String returnString() {
            return null;
        }
    }

    private TestMock service;
    private TestInterface proxy;
            
    private AbstractSerialDispatchQueue createQueue() {
        return new AbstractSerialDispatchQueue("mock queue") {
            public void dispatchAsync(Runnable runnable) {
                runnable.run();
            }
        };
    }
    
    @Test
    public void strings() throws Exception {

        final String expected1 = "hello";
        final String expected2[] = {"world"};
        
        service = new TestMock() {
            public void strings(String actual1, String[] actual2) {
                assertEquals(expected1, actual1);
                assertEquals(expected2, actual2);
            }
        };
        
        proxy = AsmActor.create(TestInterface.class, service, createQueue());
        proxy.strings(expected1, expected2);

    }

    @Test
    public void shorts() throws Exception {

        final short expected1 = 37;
        final short expected2[] = {45,37};
        
        service = new TestMock() {
            public void shorts(short actual1, short[] actual2) {
                assertEquals(expected1, actual1);
                assertEquals(expected2, actual2);
            }
        };
        
        proxy = AsmActor.create(TestInterface.class, service, createQueue());
        proxy.shorts(expected1, expected2);

    }
    
    @Test
    public void returnString() throws Exception {

        service = new TestMock() {
            public String returnString() {
                return "hello";
            }
        };
        
        proxy = AsmActor.create(TestInterface.class, service, createQueue());
        String actual = proxy.returnString();
        assertNull(actual);

    }}
