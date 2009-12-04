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

public interface Dispatcher extends Executor {

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
    
    public DispatchContext register(Runnable runnable, String name);

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
     * the future on this {@link Dispatcher}.
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