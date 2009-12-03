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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class GlobalDispatchQueue implements DispatchQueue {

    private final AdancedDispatchSystem system;
    private Executor executor;
    final String label;
    
    public GlobalDispatchQueue(AdancedDispatchSystem system, DispatchQueuePriority priority) {
        this.system = system;
        this.label=priority.toString();
        executor = this.system.pooledDispatcher.createPriorityExecutor(priority.ordinal());
    }

    public String getLabel() {
        return label;
    }

    public void dispatchAsync(Runnable runnable) {
        executor.execute(runnable);
    }

    public void dispatchAfter(long delayMS, Runnable runnable) {
        this.system.pooledDispatcher.schedule(runnable, delayMS, TimeUnit.MILLISECONDS);
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

}
