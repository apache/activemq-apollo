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

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.ExecutionLoadBalancer.LoadBalancedDispatchContext;
import org.apache.activemq.dispatch.ExecutionLoadBalancer.PoolableDispatchContext;
import org.apache.activemq.dispatch.ExecutionLoadBalancer.PoolableDispatcher;
import org.apache.activemq.queue.Mapper;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

public class PriorityDispatcher implements Runnable, PoolableDispatcher {

    private Thread thread;
    private boolean running = false;
    private boolean threaded = false;
    private final int MAX_USER_PRIORITY;

    static final ThreadLocal<PriorityDispatcher> dispatcher = new ThreadLocal<PriorityDispatcher>();

    private final ExecutionLoadBalancer loadBalancer;

    // The local dispatch queue:
    private final PriorityLinkedList<PriorityDispatchContext> priorityQueue;

    // Dispatch queue for requests from other threads:
    private final LinkedNodeList<ForeignEvent> foreignQueue = new LinkedNodeList<ForeignEvent>();

    // Timed Execution List
    private final TimerHeap timerHeap = new TimerHeap();

    private final String name;
    private final AtomicBoolean foreignAvailable = new AtomicBoolean(false);
    private final Semaphore foreignPermits = new Semaphore(0);

    private final Mapper<Integer, PriorityDispatchContext> PRIORITY_MAPPER = new Mapper<Integer, PriorityDispatchContext>() {
        public Integer map(PriorityDispatchContext element) {
            return element.listPrio;
        }
    };

    public PriorityDispatcher(String name, int priorities, ExecutionLoadBalancer loadBalancer) {
        this.name = name;
        MAX_USER_PRIORITY = priorities;
        priorityQueue = new PriorityLinkedList<PriorityDispatchContext>(MAX_USER_PRIORITY + 1, PRIORITY_MAPPER);
        this.loadBalancer = loadBalancer;
        loadBalancer.addDispatcher(this);
    }

    private abstract class ForeignEvent extends LinkedNode<ForeignEvent> {
        public abstract void execute();

        final void addToList() {
            synchronized (foreignQueue) {
                if (!this.isLinked()) {
                    foreignQueue.addLast(this);
                    if (!foreignAvailable.getAndSet(true)) {
                        foreignPermits.release();
                    }
                }
            }
        }
    }

    public boolean isThreaded() {
        return threaded;
    }

    public void setThreaded(boolean threaded) {
        this.threaded = threaded;
    }

    private class UpdateEvent extends ForeignEvent {
        private final PriorityDispatchContext pdc;

        UpdateEvent(PriorityDispatchContext pdc) {
            this.pdc = pdc;
        }

        // Can only be called by the owner of this dispatch context:
        public void execute() {
            pdc.lbContext.processForeignUpdates();
        }
    }

    class PriorityDispatchContext extends LinkedNode<PriorityDispatchContext> implements PoolableDispatchContext {
        // The dispatchable target:
        final Dispatchable dispatchable;
        LoadBalancedDispatchContext lbContext;
        // The name of this context:
        final String name;
        // list prio can only be updated in the thread of of this dispatcher:
        int listPrio;
        // The update event is used to update fields in the dispatch context
        // from foreign threads:
        final UpdateEvent updateEvent = new UpdateEvent(this);

        // Marked by the caller when this is closed.
        boolean closed = false;

        private PriorityDispatchContext(Dispatchable dispatchable, boolean persistent, String name) {
            super();
            this.dispatchable = dispatchable;
            this.name = name;
        }

        // The load balancer will guarantee that this is on our thread:
        public final void requestDispatch() {
            if (!isLinked()) {
                priorityQueue.add(this, listPrio);
            }
            return;
        }

        // The load balancer guarantees that this is called on our thread:
        public final void updatePriority(int priority) {
            if (priority != listPrio) {

                listPrio = priority;
                // If there is a priority change relink the context
                // at the new priority:
                if (isLinked()) {
                    unlink();
                    priorityQueue.add(this, listPrio);
                }
            }
            return;

        }

        public void onForeignThreadUpdate() {
            updateEvent.addToList();
        }

        // Will only be called on this thread:
        public void close() {
            if (isLinked()) {
                unlink();
            }
            synchronized (foreignQueue) {
                if (updateEvent.isLinked()) {
                    updateEvent.unlink();
                }
            }

            closed = true;
        }

        /**
         * This can only be called by the owning dispatch thread:
         * 
         * @return False if the dispatchable has more work to do.
         */
        public final boolean dispatch() {
            return dispatchable.dispatch();
        }

        public String toString() {
            return name;
        }

        public Dispatchable getDispatchable() {
            return dispatchable;
        }

        public void setLoadBalancedDispatchContext(LoadBalancedDispatchContext context) {
            this.lbContext = context;
        }

        public String getName() {
            return name;
        }

        public PoolableDispatcher getDispatcher() {
            return PriorityDispatcher.this;
        }
    }

    public DispatchContext register(Dispatchable dispatchable, String name) {
        return loadBalancer.createLoadBalancedDispatchContext(createPoolablDispatchContext(dispatchable, name));
    }

    public PoolableDispatchContext createPoolablDispatchContext(Dispatchable dispatchable, String name) {
        return new PriorityDispatchContext(dispatchable, true, name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#start()
     */
    public synchronized final void start() {
        if (thread == null) {
            running = true;
            thread = new Thread(this, name);
            thread.start();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#shutdown()
     */
    public synchronized final void shutdown() throws InterruptedException {
        if (thread != null) {
            dispatch(new RunnableAdapter(new Runnable() {

                public void run() {
                    running = false;
                }

            }), MAX_USER_PRIORITY + 1);
            thread.interrupt();
            thread.join();
            thread = null;
        }
    }

    public void run() {

        // Inform the dispatcher that we have started:
        loadBalancer.onDispatcherStarted(this);
        dispatcher.set(this);
        PriorityDispatchContext pdc;
        try {
            while (running) {
                pdc = priorityQueue.poll();
                // If no local work available wait for foreign work:
                if (pdc == null) {
                    foreignPermits.acquire();
                } else {
                    pdc.lbContext.startingDispatch();

                    while (!pdc.dispatch()) {
                        // If there is a higher priority dispatchable stop
                        // processing this one:
                        if (pdc.listPrio < priorityQueue.getHighestPriority()) {
                            // May have gotten relinked by the caller:
                            if (!pdc.isLinked()) {
                                priorityQueue.add(pdc, pdc.listPrio);
                            }
                            break;
                        }
                    }

                    pdc.lbContext.finishedDispatch();

                }

                // Execute delayed events:
                timerHeap.executeReadyEvents();

                // Check for foreign dispatch requests:
                if (foreignAvailable.get()) {
                    synchronized (foreignQueue) {
                        // Drain the foreign queue:
                        while (true) {
                            ForeignEvent fe = foreignQueue.getHead();
                            // TODO should probably swap foreign queue here:
                            if (fe == null) {
                                foreignAvailable.set(false);
                                foreignPermits.drainPermits();
                                break;
                            }

                            fe.unlink();
                            fe.execute();
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            return;
        } catch (Throwable thrown) {
            thrown.printStackTrace();
        } finally {
            loadBalancer.onDispatcherStopped(this);
        }
    }

    class ThreadSafeDispatchContext implements LoadBalancedDispatchContext {
        final PriorityDispatchContext delegate;

        ThreadSafeDispatchContext(PriorityDispatchContext context) {
            this.delegate = context;
            delegate.setLoadBalancedDispatchContext(this);
        }

        public void finishedDispatch() {
            // NOOP

        }

        public void startingDispatch() {
            // Noop

        }

        public void close() {
            // Noop this is always transient:
        }

        public void processForeignUpdates() {
            requestDispatch();
        }

        public Dispatchable getDispatchable() {
            return delegate.getDispatchable();
        }

        public void requestDispatch() {
            if (dispatcher.get() == PriorityDispatcher.this) {
                delegate.requestDispatch();
            } else {
                delegate.onForeignThreadUpdate();
            }
        }

        public void updatePriority(int priority) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public String getName() {
            return delegate.name;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.dispatch.IDispatcher#dispatch(org.apache.activemq
     * .dispatch.Dispatcher.Dispatchable)
     */
    final void dispatch(Dispatchable dispatchable, int priority) {
        ThreadSafeDispatchContext context = new ThreadSafeDispatchContext(new PriorityDispatchContext(dispatchable, false, name));
        context.delegate.updatePriority(priority);
        context.requestDispatch();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#createPriorityExecutor(int)
     */
    public Executor createPriorityExecutor(final int priority) {

        return new Executor() {

            public void execute(final Runnable runnable) {
                dispatch(new RunnableAdapter(runnable), priority);
            }
        };
    }

    public void execute(final Runnable runnable) {
        dispatch(new RunnableAdapter(runnable), 0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.dispatch.IDispatcher#schedule(java.lang.Runnable,
     * long, java.util.concurrent.TimeUnit)
     */
    public void schedule(final Runnable runnable, final long delay, final TimeUnit timeUnit) {
        if (dispatcher.get() == this) {
            timerHeap.add(runnable, delay, timeUnit);
        } else {
            new ForeignEvent() {
                public void execute() {
                    timerHeap.add(runnable, delay, timeUnit);
                }
            }.addToList();
        }
    }

    public String toString() {
        return name;
    }

    private class TimerHeap {

        final TreeMap<Long, LinkedList<Runnable>> timers = new TreeMap<Long, LinkedList<Runnable>>();

        private void add(Runnable runnable, long delay, TimeUnit timeUnit) {

            long nanoDelay = timeUnit.convert(delay, TimeUnit.NANOSECONDS);
            long eTime = System.nanoTime() + nanoDelay;
            LinkedList<Runnable> list = new LinkedList<Runnable>();
            list.add(runnable);

            LinkedList<Runnable> old = timers.put(eTime, list);
            if (old != null) {
                list.addAll(old);
            }
        }

        private void executeReadyEvents() {
            LinkedList<Runnable> ready = null;
            if (timers.isEmpty()) {
                return;
            } else {
                long now = System.nanoTime();
                long first = timers.firstKey();
                if (first > now) {
                    return;
                }
                ready = new LinkedList<Runnable>();

                while (first < now) {
                    ready.addAll(timers.remove(first));
                    if (timers.isEmpty()) {
                        break;
                    }
                    first = timers.firstKey();

                }
            }

            for (Runnable runnable : ready) {
                try {
                    runnable.run();
                } catch (Throwable thrown) {
                    thrown.printStackTrace();
                }
            }
        }
    }
}
