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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BaseRetained {
    
    final protected AtomicInteger retainCounter = new AtomicInteger(0);
    final protected ArrayList<Runnable> shutdownHandlers = new ArrayList<Runnable>(1);

    public void addShutdownWatcher(Runnable shutdownHandler) {
        synchronized(shutdownHandlers) {
            shutdownHandlers.add(shutdownHandler);
        }
    }
    
    public void retain() {
        if( retainCounter.getAndIncrement() == 0 ) {
            startup();
        }
    }

    public void release() {
        if( retainCounter.decrementAndGet()==0 ) {
            shutdown();
        }
    }
    
    protected boolean isShutdown()
    {
        return retainCounter.get() <= 0;
    }

    /**
     * Subclasses should override if they want to do some startup processing. 
     */
    protected void startup() {
    }


    /**
     * Subclasses should override if they want to do clean up. 
     */
    protected void shutdown() {
        ArrayList<Runnable> copy;
        synchronized(shutdownHandlers) {
            copy = new ArrayList<Runnable>(shutdownHandlers);
            shutdownHandlers.clear();
        }
        for (Runnable runnable : copy) {
            runnable.run();
        }
    }

}
