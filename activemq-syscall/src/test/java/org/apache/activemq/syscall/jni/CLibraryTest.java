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
 */package org.apache.activemq.syscall.jni;

import org.apache.activemq.syscall.jni.CLibrary;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.activemq.syscall.jni.CLibrary.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
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
