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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.activemq.dispatch.DispatchSystem;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class Dispatcher extends Thread {
    private final SimpleDispatchSPI system;
    
    public Dispatcher(SimpleDispatchSPI javaDispatchSystem, int ordinal) {
        system = javaDispatchSystem;
        setName("dispatcher:"+(ordinal+1));
        setDaemon(true);
        
    }
    
    @Override
    public void run() {
        GlobalDispatchQueue[] dispatchQueues = system.globalQueues;
        while( true ) {
            for (GlobalDispatchQueue queue : dispatchQueues) {
                DispatchSystem.CURRENT_QUEUE.set(queue);
                ConcurrentLinkedQueue<Runnable> runnables = queue.runnables;
                Runnable runnable;
                while( (runnable = runnables.poll())!=null ) {
                    system.globalQueuedRunnables.decrementAndGet();
                    dispatch(runnable);
                }
            }
            if( system.globalQueuedRunnables.get()==0 ) {
                try {
                    system.waitForWakeup();
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private void dispatch(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
   
}