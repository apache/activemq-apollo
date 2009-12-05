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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;



public class SimpleLoadBalancer implements LoadBalancer {

    private final boolean DEBUG = false;

    //TODO: Added plumbing for periodic re-balancing which we should
    //consider implementing
    private static final boolean ENABLE_UPDATES = false;
    private final ArrayList<DispatcherThread> dispatchers = new ArrayList<DispatcherThread>();

    private AtomicBoolean running = new AtomicBoolean(false);
    private boolean needsUpdate = false;
    private static final int FREQUENCY = 5000;
    private boolean scheduled = false;
    private final Runnable timerCallback;

    public SimpleLoadBalancer() {

        timerCallback = new Runnable() {
            public final void run() {
                if (running.get()) {
                    rebalance();
                    synchronized (dispatchers) {
                        scheduled = false;
                        scheduleNext();
                    }
                }
            }
        };

    }

    private void rebalance() {
        if (!needsUpdate) {
            return;
        }
        // TODO Auto-generated method stub
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduleNext();
        }
    }

    private void scheduleNext() {
        if (!ENABLE_UPDATES) {
            return;
        }
        synchronized (dispatchers) {
            if (!scheduled) {
                if (!dispatchers.isEmpty()) {
                    dispatchers.get(0).schedule(timerCallback, FREQUENCY, TimeUnit.MILLISECONDS);
                    scheduled = true;
                }
            }
        }
    }

    public void stop() {
        running.compareAndSet(true, false);
    }

    public synchronized final void onDispatcherStarted(DispatcherThread dispatcher) {
        dispatchers.add(dispatcher);
        scheduleNext();
    }

    /**
     * A Dispatcher must call this when exiting it's dispatch loop
     */
    public void onDispatcherStopped(DispatcherThread dispatcher) {
        dispatchers.remove(dispatcher);
    }

    public DispatchObserver createExecutionTracker(DispatchContext context) {
        return new SimpleExecutionTracker(context);
    }

    private static class ExecutionStats {
        final DispatchContext target;
        final DispatchContext source;
        int count;

        ExecutionStats(DispatchContext source, DispatchContext target) {
            this.target = target;
            this.source = source;
        }

        public String toString() {
            return "Connection from: " + source + " to " + target;
        }
    }

    private class SimpleExecutionTracker implements DispatchObserver {
        private final HashMap<DispatchContext, ExecutionStats> sources = new HashMap<DispatchContext, ExecutionStats>();
        private final DispatchContext context;
        private final AtomicInteger work = new AtomicInteger(0);

        private DispatchContext singleSource;
        private DispatcherThread currentOwner;

        SimpleExecutionTracker(DispatchContext context) {
            this.context = context;
            currentOwner = context.getTargetQueue();
        }

        /**
         * This method is called to track which dispatch contexts are requesting
         * dispatch for the target context represented by this node.
         * 
         * This method is not threadsafe, the caller must ensure serialized
         * access to this method.
         * 
         * @param callngDispatcher
         *            The calling dispatcher.
         * @param context
         *            the originating dispatch context
         * @return True if this method resulted in the dispatch request being
         *         assigned to another dispatcher.
         */
        public void onDispatch(DispatcherThread callingDispatcher, DispatchContext callingContext) {

            if (callingContext != null) {
                // Make sure we are being called by another node:
                if (callingContext == null || callingContext == context) {
                    return;
                }

                // Optimize for single source case:
                if (singleSource != callingContext) {
                    if (singleSource == null && sources.isEmpty()) {
                        singleSource = callingContext;
                        ExecutionStats stats = new ExecutionStats(callingContext, context);
                        stats.count++;
                        sources.put(callingContext, stats);

                        // If this context only has a single source
                        // assign it to that source to minimize contention:
                        if (callingDispatcher != currentOwner) {
                            if (DEBUG)
                                System.out.println("Assigning: " + context + " to " + callingContext + "'s  dispatcher: " + callingDispatcher + " From: " + currentOwner);

                            currentOwner = callingDispatcher;
                            context.setTargetQueue(callingDispatcher);
                        }

                    } else {

                        ExecutionStats stats = sources.get(callingContext);
                        if (stats == null) {
                            stats = new ExecutionStats(callingContext, context);
                            sources.put(callingContext, stats);
                        }
                        stats.count++;

                        if (singleSource != null) {
                            singleSource = null;
                        }
                    }
                }
                work.incrementAndGet();
            }
        }

        public void close() {
        }
    }
}
