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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SerialDispatchQueue extends AbstractDispatchObject implements DispatchQueue, Runnable {

    private final ConcurrentLinkedQueue<Runnable> runnables = new ConcurrentLinkedQueue<Runnable>();
    final private String label;
    final private AtomicInteger suspendCounter = new AtomicInteger();
    final private AtomicLong size = new AtomicLong();

    public SerialDispatchQueue(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public void resume() {
        suspendCounter.decrementAndGet();
    }

    public void suspend() {
        suspendCounter.incrementAndGet();
    }

    public void dispatchAfter(long delayMS, Runnable runnable) {
        throw new RuntimeException("TODO: implement me.");
    }

    public void dispatchAsync(Runnable runnable) {
        if( runnable == null ) {
            throw new IllegalArgumentException();
        }
        long lastSize = size.incrementAndGet();
        runnables.add(runnable);
        if( targetQueue!=null && lastSize == 1 && suspendCounter.get()<=0 ) {
            targetQueue.dispatchAsync(this);
        }
    }

    public void run() {
        DispatchQueue original = AdancedDispatchSystem.CURRENT_QUEUE.get();
        AdancedDispatchSystem.CURRENT_QUEUE.set(this);
        try {
            Runnable runnable;
            long lsize = size.get();
            while( suspendCounter.get() <= 0 && lsize > 0 ) {
                try {
                    runnable = runnables.poll();
                    if( runnable!=null ) {
                        runnable.run();
                        lsize = size.decrementAndGet();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        } finally {
            AdancedDispatchSystem.CURRENT_QUEUE.set(original);
        }
    }

    public void dispatchSync(Runnable runnable) throws InterruptedException {
        dispatchApply(1, runnable);
    }
    
    public void dispatchApply(int iterations, Runnable runnable) throws InterruptedException {
        QueueSupport.dispatchApply(this, iterations, runnable);
    }
}
