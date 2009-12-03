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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public interface IDispatcher extends Executor {

    /**
     * This interface is implemented by Dispatchable entities. A Dispatchable
     * entity registers with an {@link IDispatcher} and is returned a
     * {@link DispatchContext} which it can use to request the
     * {@link IDispatcher} to invoke {@link Dispatchable#dispatch()}
     * 
     * {@link IDispatcher} guarantees that {@link #dispatch()} will never invoke
     * dispatch concurrently unless the {@link Dispatchable} is registered with
     * more than one {@link IDispatcher};
     */
    public interface Dispatchable {
        public boolean dispatch();
    }

    /**
     * Returned to callers registered with this dispathcer. Used by the caller
     * to inform the dispatcher that it is ready for dispatch.
     * 
     * Note that DispatchContext is not safe for concurrent access by multiple
     * threads.
     */
    public interface DispatchContext {
        /**
         * Once registered with a dispatcher, this can be called to request
         * dispatch. The {@link Dispatchable} will remain in the dispatch queue
         * until a subsequent call to {@link Dispatchable#dispatch()} returns
         * false;
         * 
         * @throws RejectedExecutionException If the dispatcher has been shutdown.
         */
        public void requestDispatch() throws RejectedExecutionException;

        /**
         * This can be called to update the dispatch priority.
         * 
         * @param priority
         */
        public void updatePriority(int priority);

        /**
         * Gets the Dispatchable that this context represents.
         * 
         * @return The dispatchable
         */
        public Dispatchable getDispatchable();

        /**
         * Gets the name of the dispatch context
         * 
         * @return The dispatchable
         */
        public String getName();

        /**
         * This must be called to release any resource the dispatcher is holding
         * on behalf of this context. Once called this {@link DispatchContext} should
         * no longer be used. 
         */
        public void close(boolean sync);
    }

    public class RunnableAdapter implements Dispatchable, Runnable {
        private Runnable runnable;

        public RunnableAdapter() {
            runnable = this;
        }
        public RunnableAdapter(Runnable runnable) {
            this.runnable = runnable;
        }

        public boolean dispatch() {
            runnable.run();
            return true;
        }

        public void run() {
        }
    }

    /**
     * Registers a {@link Dispatchable} with this dispatcher, and returns a
     * {@link DispatchContext} that the caller can use to request dispatch.
     * 
     * @param dispatchable
     *            The {@link Dispatchable}
     * @param name
     *            An identifier for the dispatcher.
     * @return A {@link DispatchContext} that can be used to request dispatch
     */
    public DispatchContext register(Dispatchable dispatchable, String name);

    /**
     * Gets the number of dispatch priorities. Dispatch priorities are 0 based, 
     * so if the number of dispatch priorities is 10, the maxium is 9.
     * @return the number of dispatch priorities.
     */
    public int getDispatchPriorities();
    
    /**
     * Creates an executor that will execute its tasks at the specified
     * priority.
     * 
     * @param priority
     *            The priority
     * @return A prioritized executor.
     */
    public Executor createPriorityExecutor(int priority);

    /**
     * Starts the dispatcher.
     */
    public void start() throws Exception;

    /**
     * Shuts down the dispatcher, this may result in previous dispatch requests
     * going unserved.
     */
    public void shutdown() throws InterruptedException;

    /**
     * Schedules the given {@link Runnable} to be run at the specified time in
     * the future on this {@link IDispatcher}.
     * 
     * @param runnable
     *            The Runnable to execute
     * @param delay
     *            The delay
     * @param timeUnit
     *            The TimeUnit used to interpret delay.
     */
    public void schedule(final Runnable runnable, long delay, TimeUnit timeUnit);

}