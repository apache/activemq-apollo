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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.dispatch.DispatchSystem;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class DispatcherThread extends Thread {
    private static final int MAX_DISPATCH_BEFORE_CHECKING_FOR_HIGHER_PRIO = 10000;
    private final SimpleDispatchSPI spi;
    final ThreadDispatchQueue[] threadQueues;
    final AtomicLong threadQueuedRunnables = new AtomicLong();
        
    public DispatcherThread(SimpleDispatchSPI spi, int ordinal) {
        this.spi = spi;
        this.threadQueues = new ThreadDispatchQueue[3];
        for (int i = 0; i < 3; i++) {
            threadQueues[i] = new ThreadDispatchQueue(this, DispatchPriority.values()[i] );
        }
        setName(spi.getLabel()+" dispatcher: "+(ordinal+1));
        setDaemon(true);
    }
    
    @Override
    public void run() {
        try {
            outer: while( true ) {
                int counter=0;
                for (SimpleQueue queue : threadQueues) {
                    DispatchSystem.CURRENT_QUEUE.set(queue);
                    Runnable runnable;
                    while( (runnable = queue.poll())!=null ) {
                        dispatch(runnable);
                        counter++;
                    }
                }
                if( counter!=0 ) {
                    // don't service the global queues until the thread queues are 
                    // drained.
                    continue;
                }
                
                for (SimpleQueue queue : spi.globalQueues) {
                    DispatchSystem.CURRENT_QUEUE.set(threadQueues[queue.getPriority().ordinal()]);
                    
                    Runnable runnable;
                    while( (runnable = queue.poll())!=null ) {
                        dispatch(runnable);
                        counter++;
                        
                        // Thread queues have the priority.
                        if( threadQueuedRunnables.get()!=0 ) {
                            continue outer;
                        }
                    }
                }
                if( counter!=0 ) {
                    // don't wait for wake up until we could find 
                    // no runnables to dispatch.
                    continue;
                }
            
//        GlobalDispatchQueue[] globalQueues = spi.globalQueues;
//        while( true ) {
//
//            if( dispatch(threadQueues[0]) 
//                || dispatch(globalQueues[0]) 
//                || dispatch(threadQueues[1]) 
//                || dispatch(globalQueues[1]) 
//                || dispatch(threadQueues[2]) 
//                || dispatch(globalQueues[2]) 
//                ) {
//                continue;
//            }
//        
                try {
                    waitForWakeup();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        } catch (Shutdown e) {
        }
    }
    
    @SuppressWarnings("serial")
    static class Shutdown extends RuntimeException {
    }

    private boolean dispatch(SimpleQueue queue) {
        int counter=0;
        Runnable runnable;
        while( counter < MAX_DISPATCH_BEFORE_CHECKING_FOR_HIGHER_PRIO ) {
            runnable = queue.poll();
            if( runnable == null ) {
                break;
            }        
            if( counter==0 ) {
                DispatchSystem.CURRENT_QUEUE.set(queue);
            }
            dispatch(runnable);
            counter++;
        }
        return counter!=0;
    }

    private void dispatch(Runnable runnable) {
        try {
            runnable.run();
        } catch (Shutdown e) {
            throw e;
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private final Object wakeupMutex = new Object();
    private boolean inWaitingList;
    
    private void waitForWakeup() throws InterruptedException {
        while( threadQueuedRunnables.get()==0 && spi.globalQueuedRunnables.get()==0 ) {
            synchronized(wakeupMutex) {
                if( !inWaitingList ) {
                    spi.addWaitingDispatcher(this);
                    inWaitingList=true;
                }
                wakeupMutex.wait();
            }
        }
    }

    public void globalWakeup() {
        synchronized(wakeupMutex) {
            inWaitingList=false;
            wakeupMutex.notify();
        }
    }
    
    public void wakeup() {
        synchronized(wakeupMutex) {
            wakeupMutex.notify();
        }
    }
   
}