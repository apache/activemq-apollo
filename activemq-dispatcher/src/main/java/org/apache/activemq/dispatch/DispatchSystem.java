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
package org.apache.activemq.dispatch;

import java.nio.channels.SelectableChannel;

import org.apache.activemq.dispatch.internal.simple.SimpleDispatchSystem;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DispatchSystem {

    private static final SimpleDispatchSystem system = new SimpleDispatchSystem(Runtime.getRuntime().availableProcessors());
    
    static DispatchQueue getMainQueue() {
        return system.getMainQueue();
    }
    
    public static enum DispatchQueuePriority {
        HIGH,
        DEFAULT,
        LOW;
    }

    static public DispatchQueue getGlobalQueue(DispatchQueuePriority priority) {
        return system.getGlobalQueue(priority);
    }
    
    static DispatchQueue createQueue(String label) {
        return system.createQueue(label);
    }
    
    static DispatchQueue getCurrentQueue() {
        return system.getCurrentQueue();
    }
    
    static void dispatchMain() {
        system.dispatchMain();
    }

    static DispatchSource createSource(SelectableChannel channel, int interestOps, DispatchQueue queue) {
        return system.createSource(channel, interestOps, queue);
    }
    
    

}
