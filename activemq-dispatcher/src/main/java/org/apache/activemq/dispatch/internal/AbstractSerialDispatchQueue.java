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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchOption;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class AbstractSerialDispatchQueue extends AbstractDispatchObject implements DispatchQueue, Runnable {

    protected final String label;
    protected final AtomicInteger suspendCounter = new AtomicInteger();
    
    protected final ConcurrentLinkedQueue<Runnable> runnables = new ConcurrentLinkedQueue<Runnable>();
    
    protected final AtomicLong size = new AtomicLong();
    protected final Set<DispatchOption> options;

    public AbstractSerialDispatchQueue(String label, DispatchOption...options) {
        this.label = label;
        this.options = set(options);
        retain();
    }

    static private Set<DispatchOption> set(DispatchOption[] options) {
        if( options==null || options.length==0 )
            return Collections.emptySet() ;
        return Collections.unmodifiableSet(EnumSet.copyOf(Arrays.asList(options)));
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
    }

    public void dispatchSync(Runnable runnable) throws InterruptedException {
        dispatchApply(1, runnable);
    }
    
    public void dispatchApply(int iterations, Runnable runnable) throws InterruptedException {
        QueueSupport.dispatchApply(this, iterations, runnable);
    }

    public Set<DispatchOption> getOptions() {
        return options;
    }

    
}
