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
package org.apache.activemq.dispatch.internal.simple;

import java.nio.channels.SelectableChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.dispatch.Dispatch;
import org.apache.activemq.dispatch.DispatchSource;
import org.apache.activemq.dispatch.internal.BaseRetained;
import org.apache.activemq.dispatch.internal.SerialDispatchQueue;

import static org.apache.activemq.dispatch.DispatchPriority.*;



/**
 * Implements a simple dispatch system.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SimpleDispatchSPI extends BaseRetained implements Dispatch {
        
    public final static ThreadLocal<DispatchQueue> CURRENT_QUEUE = new ThreadLocal<DispatchQueue>();

    final SerialDispatchQueue mainQueue = new SerialDispatchQueue("main");
    final GlobalDispatchQueue globalQueues[]; 
    final DispatcherThread dispatchers[];
    final AtomicLong globalQueuedRunnables = new AtomicLong();
    
    final ConcurrentLinkedQueue<DispatcherThread> waitingDispatchers = new ConcurrentLinkedQueue<DispatcherThread>();
    final AtomicInteger waitingDispatcherCount = new AtomicInteger();
    final AtomicInteger startCounter = new AtomicInteger();
    private final String label;
    TimerThread timerThread;
    
    public SimpleDispatchSPI(String label, int size) {
        this.label = label;
        globalQueues = new GlobalDispatchQueue[3];
        for (int i = 0; i < 3; i++) {
            globalQueues[i] = new GlobalDispatchQueue(this, DispatchPriority.values()[i] );
        }
        dispatchers = new DispatcherThread[size];
    }

    public DispatchQueue getMainQueue() {
        return mainQueue;
    }
    
    public DispatchQueue getGlobalQueue() {
        return getGlobalQueue(DEFAULT);
    }

    public DispatchQueue getGlobalQueue(DispatchPriority priority) {
        return globalQueues[priority.ordinal()];
    }
    
    public DispatchQueue createSerialQueue(String label) {
        SerialDispatchQueue rc = new SerialDispatchQueue(label) {
            @Override
            public void dispatchAfter(Runnable runnable, long delay, TimeUnit unit) {
                timerThread.addRelative(runnable, this, delay, unit);
            }
        };
        rc.setTargetQueue(getGlobalQueue());
        return rc;
    }
    
    public void dispatchMain() {
        mainQueue.run();
    }

    public DispatchSource createSource(SelectableChannel channel, int interestOps, DispatchQueue queue) {
        return null;
    }

    public void addWaitingDispatcher(DispatcherThread dispatcher) {
        waitingDispatcherCount.incrementAndGet();
        waitingDispatchers.add(dispatcher);
    }
    
    public void wakeup() {
        int value = waitingDispatcherCount.get();
        if( value!=0 ) {
            DispatcherThread dispatcher = waitingDispatchers.poll();
            if( dispatcher!=null ) {
                waitingDispatcherCount.decrementAndGet();
                dispatcher.globalWakeup();
            }
        }
    }

    public void start() {
        if( startCounter.getAndIncrement()==0 ) {
            for (int i = 0; i < dispatchers.length; i++) {
                dispatchers[i] = new DispatcherThread(this, i);
                dispatchers[i].start();
            }
            timerThread = new TimerThread(this);
            timerThread.start();
        }
    }

    public void shutdown(final Runnable onShutdown) {
        if( startCounter.decrementAndGet()==0 ) {
            
            final AtomicInteger shutdownCountDown = new AtomicInteger(dispatchers.length+1);
            Runnable wrapper = new Runnable() {
                public void run() {
                    if( shutdownCountDown.decrementAndGet()==0 && onShutdown!=null) {
                        onShutdown.run();
                    }
                    throw new DispatcherThread.Shutdown();
                }
            };

            timerThread.shutdown(wrapper);
            for (int i = 0; i < dispatchers.length; i++) {
                ThreadDispatchQueue queue = dispatchers[i].threadQueues[LOW.ordinal()];
                queue.runnables.add(wrapper);
            }
        }
    }

    public String getLabel() {
        return label;
    }

    public DispatchQueue getCurrentQueue() {
        return CURRENT_QUEUE.get();
    }
    
}
