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

import org.apache.activemq.dispatch.internal.advanced.ExecutionLoadBalancer.ExecutionTracker;
import org.apache.activemq.dispatch.internal.advanced.IDispatcher.DispatchContext;

public interface PooledDispatcher {

    /**
     * A {@link PooledDispatchContext}s can be moved between different
     * dispatchers.
     */
    public interface PooledDispatchContext extends DispatchContext {
        /**
         * Called to transfer a {@link PooledDispatchContext} to a new
         * Dispatcher.
         */
        public void assignToNewDispatcher(IDispatcher newDispatcher);

        /**
         * Gets the dispatcher to which this PooledDispatchContext currently
         * belongs
         * 
         * @return
         */
        public IDispatcher getDispatcher();

        /**
         * Gets the execution tracker for the context.
         * 
         * @return the execution tracker for the context:
         */
        public ExecutionTracker getExecutionTracker();
    }

    /**
     * A Dispatcher must call this from it's dispatcher thread to indicate that
     * is has started it's dispatch has started.
     */
    public void onDispatcherStarted(IDispatcher dispatcher);

    /**
     * A Dispatcher must call this from it's dispatcher thread when exiting it's
     * dispatch loop
     */
    public void onDispatcherStopped(IDispatcher dispatcher);

    /**
     * Returns the currently executing dispatcher, or null if the current thread
     * is not a dispatcher:
     * 
     * @return The currently executing dispatcher
     */
    public IDispatcher getCurrentDispatcher();

    public void setCurrentDispatchContext(PooledDispatchContext context);

    public PooledDispatchContext getCurrentDispatchContext();

    /**
     * Returns the load balancer for this dispatch pool.
     * 
     * @return
     */
    public ExecutionLoadBalancer getLoadBalancer();
}
