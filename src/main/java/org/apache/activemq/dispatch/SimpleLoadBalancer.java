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
package org.apache.activemq.dispatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.dispatch.IDispatcher.Dispatchable;

/**
 * 
 */
public class SimpleLoadBalancer implements ExecutionLoadBalancer {

    private static final ThreadLocal<ExecutionGraphNode> dispatchContext = new ThreadLocal<ExecutionGraphNode>();
    private static final ThreadLocal<PoolableDispatcher> dispatcher = new ThreadLocal<PoolableDispatcher>();

    private final ArrayList<PoolableDispatcher> dispatchers = new ArrayList<PoolableDispatcher>();

    private final String name;
    private final boolean DEBUG = false;

    SimpleLoadBalancer(String name) {
        this.name = name;
    }

    public void start() throws InterruptedException {
    }

    public void shutdown() {
    }

    public LoadBalancedDispatchContext createLoadBalancedDispatchContext(PoolableDispatchContext context) {
        ExecutionGraphNode egn = new ExecutionGraphNode(context);
        return egn;
    }

    public synchronized final void addDispatcher(PoolableDispatcher dispatcher) {
        dispatchers.add(dispatcher);
    }

    /**
     * A Dispatcher must call this to indicate that is has started it's dispatch
     * loop.
     */
    public void onDispatcherStarted(PoolableDispatcher d) {
        dispatcher.set(d);
    }

    /**
     * A Dispatcher must call this when exiting it's dispatch loop
     */
    public void onDispatcherStopped(PoolableDispatcher d) {

    }

    private class ExecutionGraphEdge {
        final ExecutionGraphNode target;
        final ExecutionGraphNode source;
        int count;

        ExecutionGraphEdge(ExecutionGraphNode source, ExecutionGraphNode target) {
            this.target = target;
            this.source = source;
        }

        public String toString() {
            return "Connection from: " + source + " to " + target;
        }
    }

    /**
     * ExecutionGraphNode tracks dispatch information for a
     * MappableDispatchContext.
     * 
     */
    public class ExecutionGraphNode implements LoadBalancedDispatchContext {
        protected PoolableDispatchContext context;
        private ExecutionGraphNode singleSource;
        private final HashMap<ExecutionGraphNode, ExecutionGraphEdge> sources = new HashMap<ExecutionGraphNode, ExecutionGraphEdge>();
        protected PoolableDispatcher currentOwner;
        private final AtomicInteger work = new AtomicInteger(0);

        private int priority;
        private boolean dispatchRequested = false;
        private PoolableDispatcher updateDispatcher = null;

        ExecutionGraphNode(PoolableDispatchContext context) {
            this.context = context;
            this.context.setLoadBalancedDispatchContext(this);
            this.currentOwner = context.getDispatcher();
            if (DEBUG) {
                System.out.println(getName() + " Assigned to " + context.getDispatcher());
            }
        }

        public final void startingDispatch() {
            dispatchContext.set(this);
        }

        public final void finishedDispatch() {
            dispatchContext.set(null);
        }

        /**
         * This method is called to track which dispatch contexts are requesting
         * dispatch for the target context represented by this node.
         * 
         * This method is not threadsafe, the caller must ensure serialized
         * access to this method.
         * 
         * @param callingDispatcher
         *            The calling dispatcher.
         * @return True if this method resulted in the dispatch request being
         *         assigned to another dispatcher.
         */
        public final boolean onDispatchRequest(final PoolableDispatcher callingDispatcher) {

            /*
             * if (callingDispatcher == currentOwner) { return false; }
             */

            ExecutionGraphNode callingContext = dispatchContext.get();
            if (callingContext != null) {
                // Make sure we are being called by another node:
                if (callingContext == null || callingContext == context) {
                    return false;
                }

                // Optimize for single source case:
                if (singleSource != callingContext) {
                    if (singleSource == null && sources.isEmpty()) {
                        singleSource = callingContext;
                        ExecutionGraphEdge edge = new ExecutionGraphEdge(callingContext, this);
                        sources.put(callingContext, edge);

                        // If this context only has a single source
                        // immediately assign it to the
                        // dispatcher of the source:
                        boolean reassigned = false;
                        synchronized (this) {
                            if (callingDispatcher != currentOwner && updateDispatcher == null) {
                                updateDispatcher = callingDispatcher;
                                reassigned = true;
                                if (DEBUG)
                                    System.out.println("Assigning: " + this + " to " + callingContext + "'s  dispatcher: " + callingDispatcher);

                            }
                        }
                        if (reassigned) {
                            assignToNewDispatcher(callingDispatcher);
                        }
                        return true;
                    } else {

                        ExecutionGraphEdge stats = sources.get(callingContext);
                        if (stats == null) {
                            stats = new ExecutionGraphEdge(callingContext, this);
                            sources.put(callingContext, stats);
                        }

                        if (singleSource != null) {
                            singleSource = null;
                        }
                    }
                }
                work.incrementAndGet();
            }
            return false;
        }

        final void assignToNewDispatcher(PoolableDispatcher newDispatcher) {
            synchronized (this) {
                if (newDispatcher != currentOwner) {
                    updateDispatcher = newDispatcher;
                }
            }
            context.onForeignThreadUpdate();
        }

        public void requestDispatch() {

            PoolableDispatcher callingDispatcher = dispatcher.get();

            if (onDispatchRequest(callingDispatcher)) {
                return;
            }

            // Otherwise this is coming off another thread, so we need to
            // synchronize
            // to protect against ownership changes:
            synchronized (this) {
                // If the owner of this context is the calling thread, then
                // delegate to the dispatcher.
                if (currentOwner == callingDispatcher) {

                    context.requestDispatch();
                    return;
                }

                dispatchRequested = true;
            }
            context.onForeignThreadUpdate();
        }

        public void updatePriority(int priority) {
            if (this.priority == priority) {
                return;
            }
            // Otherwise this is coming off another thread, so we need to
            // synchronize
            // to protect against ownership changes:
            synchronized (this) {
                this.priority = priority;

                IDispatcher callingDispatcher = dispatcher.get();

                // If the owner of this context is the calling thread, then
                // delegate to the dispatcher.
                if (currentOwner == callingDispatcher) {

                    context.updatePriority(priority);
                    return;
                }
            }
            context.onForeignThreadUpdate();
        }

        public void processForeignUpdates() {
            boolean ownerChange = false;
            synchronized (this) {
                if (updateDispatcher != null) {
                    // Close the old context:
                    if (DEBUG) {
                        System.out.println("Assigning " + getName() + " to " + updateDispatcher);
                    }
                    context.close();

                    currentOwner = updateDispatcher;
                    updateDispatcher = null;
                    context = currentOwner.createPoolablDispatchContext(context.getDispatchable(), context.getName());
                    dispatchRequested = true;
                    context.updatePriority(priority);
                    context.setLoadBalancedDispatchContext(this);
                    ownerChange = true;
                } else {
                    context.updatePriority(priority);

                    if (dispatchRequested) {
                        context.requestDispatch();
                        dispatchRequested = false;
                    }
                }
            }

            if (ownerChange) {
                context.onForeignThreadUpdate();
            }
        }

        public void close() {
            sources.clear();
        }

        public final String toString() {
            return context.toString();
        }

        public Dispatchable getDispatchable() {
            return context.getDispatchable();
        }

        public String getName() {
            return context.getName();
        }
    }
}
