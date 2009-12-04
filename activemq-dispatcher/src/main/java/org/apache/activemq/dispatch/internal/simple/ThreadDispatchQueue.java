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

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority;
import org.apache.activemq.dispatch.internal.QueueSupport;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ThreadDispatchQueue implements SimpleQueue {

    final String label;
    final LinkedList<Runnable> localRunnables = new LinkedList<Runnable>();
    final ConcurrentLinkedQueue<Runnable> runnables = new ConcurrentLinkedQueue<Runnable>();
    private DispatcherThread dispatcher;
    final AtomicLong counter;
    private final DispatchQueuePriority priority;
    
    public ThreadDispatchQueue(DispatcherThread dispatcher, DispatchQueuePriority priority) {
        this.dispatcher = dispatcher;
        this.priority = priority;
        this.label=priority.toString();
        this.counter = dispatcher.threadQueuedRunnables;
    }

    public String getLabel() {
        return label;
    }

    public void dispatchAsync(Runnable runnable) {
        // We don't have to take the synchronization hit 
        // if the current thread is the dispatcher since we know it's not
        // waiting.
        if( Thread.currentThread()!=dispatcher ) {
            counter.incrementAndGet();
            runnables.add(runnable);
            dispatcher.wakeup();
        } else {
            localRunnables.add(runnable);
        }
    }

    public Runnable poll() {
        
        // This method should only be called by our dispatcher 
        // thread.
        assert Thread.currentThread()==dispatcher;
        
        Runnable rc = localRunnables.poll();
        if( rc !=null ) {
            return rc;
        }
        
        rc = runnables.poll();
        if( rc !=null ) {
            counter.decrementAndGet();
        }
        return rc;
    }

    public void dispatchAfter(long delayMS, Runnable runnable) {
        throw new RuntimeException("TODO: implement me.");
    }

    public void dispatchSync(final Runnable runnable) throws InterruptedException {
        dispatchApply(1, runnable);
    }
    
    public void dispatchApply(int iterations, final Runnable runnable) throws InterruptedException {
        QueueSupport.dispatchApply(this, iterations, runnable);
    }

    public void resume() {
        throw new UnsupportedOperationException();
    }

    public void suspend() {
        throw new UnsupportedOperationException();
    }

    public <Context> Context getContext() {
        throw new UnsupportedOperationException();
    }

    public <Context> void setContext(Context context) {
        throw new UnsupportedOperationException();
    }

    public void setFinalizer(Runnable finalizer) {
        throw new UnsupportedOperationException();
    }

    public void setTargetQueue(DispatchQueue queue) {
        throw new UnsupportedOperationException();
    }
    
    public DispatchQueuePriority getPriority() {
        return priority;
    }

}
