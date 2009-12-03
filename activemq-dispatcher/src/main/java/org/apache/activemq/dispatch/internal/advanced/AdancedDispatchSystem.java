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
package org.apache.activemq.dispatch.internal.advanced;

import java.nio.channels.SelectableChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSource;
import org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority;

import static org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority.*;


/**
 * Implements a simple dispatch system.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class AdancedDispatchSystem {
    
    static final ThreadLocal<DispatchQueue> CURRENT_QUEUE = new ThreadLocal<DispatchQueue>();
    
    final SerialDispatchQueue mainQueue = new SerialDispatchQueue("main");
    final GlobalDispatchQueue globalQueues[];
    PooledPriorityDispatcher pooledDispatcher;

    final AtomicLong globalQueuedRunnables = new AtomicLong();
    
    public AdancedDispatchSystem(int size) throws Exception {
        pooledDispatcher = new PooledPriorityDispatcher("default", size, 3);
        pooledDispatcher.start();

        globalQueues = new GlobalDispatchQueue[3];
        for (int i = 0; i < 3; i++) {
            globalQueues[i] = new GlobalDispatchQueue(this, DispatchQueuePriority.values()[i]);
        }
    }
    
    public DispatchQueue getMainQueue() {
        return mainQueue;
    }
    
    public DispatchQueue getGlobalQueue(DispatchQueuePriority priority) {
        return globalQueues[priority.ordinal()];
    }
    
    public DispatchQueue createQueue(String label) {
        SerialDispatchQueue rc = new SerialDispatchQueue(label);
        rc.setTargetQueue(getGlobalQueue(DEFAULT));
        return rc;
    }
    
    public DispatchQueue getCurrentQueue() {
        return CURRENT_QUEUE.get();
    }
    
    public void dispatchMain() {
        mainQueue.run();
    }

    public DispatchSource createSource(SelectableChannel channel, int interestOps, DispatchQueue queue) {
        return null;
    }    
}
