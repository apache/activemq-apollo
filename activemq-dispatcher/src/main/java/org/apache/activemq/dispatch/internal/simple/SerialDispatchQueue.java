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
import java.util.concurrent.TimeUnit;

import org.apache.activemq.dispatch.DispatchOption;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;

public final class SerialDispatchQueue extends AbstractSerialDispatchQueue implements SimpleQueue {

    private final SimpleDispatcher dispatcher;
    private volatile boolean stickToThreadOnNextDispatch; 
    private volatile boolean stickToThreadOnNextDispatchRequest; 
    private final LinkedList<Runnable> localEnqueues = new LinkedList<Runnable>();
    private final ThreadLocal<Boolean> executing = new ThreadLocal<Boolean>();
    
    SerialDispatchQueue(SimpleDispatcher dispatcher, String label, DispatchOption...options) {
        super(label, options);
        this.dispatcher = dispatcher;
        if( getOptions().contains(DispatchOption.STICK_TO_DISPATCH_THREAD) ) {
            stickToThreadOnNextDispatch=true;
        }
    }

    @Override
    public void setTargetQueue(DispatchQueue targetQueue) {
        GlobalDispatchQueue global = ((SimpleQueue)targetQueue).isGlobalDispatchQueue(); 
        if( getOptions().contains(DispatchOption.STICK_TO_CALLER_THREAD) && global!=null ) {
            stickToThreadOnNextDispatchRequest=true;
        }
        super.setTargetQueue(targetQueue);
    }
    
    @Override
    public void dispatchAsync(Runnable runnable) {
        
        if( stickToThreadOnNextDispatchRequest ) {
            SimpleQueue current = SimpleDispatcher.CURRENT_QUEUE.get();
            if( current!=null ) {
                SimpleQueue parent;
                while( (parent = current.getTargetQueue()) !=null ) {
                    current = parent;
                }
                if( current.isThreadDispatchQueue()==null ) {
                    System.out.println("crap");
                }
                super.setTargetQueue(current);
                stickToThreadOnNextDispatchRequest=false;
            }
        }

        // We can take a shortcut...
        if( executing.get()!=null ) {
            localEnqueues.add(runnable);
        } else {
            super.dispatchAsync(runnable);
        }
    }
    
    public void run() {
        SimpleQueue current = SimpleDispatcher.CURRENT_QUEUE.get();
        if( stickToThreadOnNextDispatch ) {
            stickToThreadOnNextDispatch=false;
            GlobalDispatchQueue global = current.isGlobalDispatchQueue();
            if( global!=null ) {
                setTargetQueue(global.getTargetQueue());
            }
        }
        
        DispatcherThread thread = DispatcherThread.currentDispatcherThread();
        
        SimpleDispatcher.CURRENT_QUEUE.set(this);
        executing.set(true);
        try {
            
            Runnable runnable;
            long lsize = size.get();
            while( suspendCounter.get() <= 0 && lsize > 0 ) {
                
                runnable = runnables.poll();
                if( runnable!=null ) {
                    runnable.run();
                    lsize = size.decrementAndGet();
                    if( lsize==0 ) {
                        release();
                    }
                    if( thread.executionCounter.decrementAndGet() <= 0 ) {
                        return;
                    }
                }
            }
            
            while( (runnable = localEnqueues.poll())!=null ) {
                runnable.run();
                if( thread.executionCounter.decrementAndGet() <= 0 ) {
                    return;
                }
            }
            
        } finally {
            executing.remove();
            
            if( !localEnqueues.isEmpty() ) {
                
                long lastSize = size.getAndAdd(localEnqueues.size());
                if( lastSize==0 ) {
                    retain();
                }
                runnables.addAll(localEnqueues);
                localEnqueues.clear();
                
                if( suspendCounter.get()<=0 ) {
                    dispatchSelfAsync();
                }
            }
            SimpleDispatcher.CURRENT_QUEUE.set(current);
        }
    }

    @Override
    public void dispatchAfter(Runnable runnable, long delay, TimeUnit unit) {
        dispatcher.timerThread.addRelative(runnable, this, delay, unit);
    }

    public DispatchPriority getPriority() {
        throw new UnsupportedOperationException();
    }

    public Runnable poll() {
        throw new UnsupportedOperationException();
    }

    public GlobalDispatchQueue isGlobalDispatchQueue() {
        return null;
    }

    public SerialDispatchQueue isSerialDispatchQueue() {
        return this;
    }

    public ThreadDispatchQueue isThreadDispatchQueue() {
        return null;
    }
    
    public SimpleQueue getTargetQueue() {
        return (SimpleQueue) targetQueue;
    }

}