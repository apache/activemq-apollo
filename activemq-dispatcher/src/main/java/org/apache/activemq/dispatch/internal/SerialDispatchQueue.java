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
package org.apache.activemq.dispatch.internal;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatch;
import org.apache.activemq.dispatch.DispatchFactory;

import static org.apache.activemq.dispatch.DispatchFactory.*;

import static org.apache.activemq.dispatch.DispatchFactory.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SerialDispatchQueue extends AbstractDispatchObject implements DispatchQueue, Runnable {

    private final ConcurrentLinkedQueue<Runnable> runnables = new ConcurrentLinkedQueue<Runnable>();
    final private String label;
    final private AtomicInteger suspendCounter = new AtomicInteger();
    final private AtomicLong size = new AtomicLong();
    
    static final ThreadLocal<DispatchQueue> CURRENT_QUEUE = new ThreadLocal<DispatchQueue>();

    public SerialDispatchQueue(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public void resume() {
        if( suspendCounter.decrementAndGet() == 0 ) {
            if( size.get() != 0 ) {
                dispatchSelfAsync();
            }
        }
    }

    public void suspend() {
        suspendCounter.incrementAndGet();
    }

    public void dispatchAfter(Runnable runnable, long delay, TimeUnit unit) {
        throw new RuntimeException("TODO: implement me.");
    }

    public void execute(Runnable command) {
        dispatchAsync(command);
    }

    public void dispatchAsync(Runnable runnable) {
        if( runnable == null ) {
            throw new IllegalArgumentException();
        }
        long lastSize = size.getAndIncrement();
        if( lastSize==0 ) {
            retain();
        }
        runnables.add(runnable);
        if( lastSize == 0 && suspendCounter.get()<=0 ) {
            dispatchSelfAsync();
        }
    }

    protected void dispatchSelfAsync() {
        targetQueue.dispatchAsync(this);
    }

    public void run() {
        DispatchQueue original = CURRENT_QUEUE.get();
        CURRENT_QUEUE.set(this);
        try {
            Runnable runnable;
            long lsize = size.get();
            while( suspendCounter.get() <= 0 && lsize > 0 ) {
                try {
                    runnable = runnables.poll();
                    if( runnable!=null ) {
                        runnable.run();
                        lsize = size.decrementAndGet();
                        if( lsize==0 ) {
                            release();
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        } finally {
            CURRENT_QUEUE.set(original);
        }
    }

    public void dispatchSync(Runnable runnable) throws InterruptedException {
        dispatchApply(1, runnable);
    }
    
    public void dispatchApply(int iterations, Runnable runnable) throws InterruptedException {
        QueueSupport.dispatchApply(this, iterations, runnable);
    }

}
