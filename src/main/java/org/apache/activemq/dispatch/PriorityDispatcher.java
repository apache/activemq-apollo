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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.ExecutionLoadBalancer.ExecutionTracker;
import org.apache.activemq.dispatch.PooledDispatcher.PooledDispatchContext;
import org.apache.activemq.queue.Mapper;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

public class PriorityDispatcher<D extends PriorityDispatcher<D>> implements Runnable, IDispatcher {

    private static final boolean DEBUG = false;
    private Thread thread;
    protected boolean running = false;
    private boolean threaded = false;
    protected final int MAX_USER_PRIORITY;

    // Set if this dispatcher is part of a dispatch pool:
    protected final PooledDispatcher<D> pooledDispatcher;

    // The local dispatch queue:
    private final PriorityLinkedList<PriorityDispatchContext> priorityQueue;

    // Dispatch queue for requests from other threads:
    private final LinkedNodeList<ForeignEvent>[] foreignQueue;
    private static final int[] TOGGLE = new int[] { 1, 0 };
    private int foreignToggle = 0;

    // Timed Execution List
    protected final TimerHeap timerHeap = new TimerHeap();

    protected final String name;
    private final AtomicBoolean foreignAvailable = new AtomicBoolean(false);
    private final Semaphore foreignPermits = new Semaphore(0);

    private final Mapper<Integer, PriorityDispatchContext> PRIORITY_MAPPER = new Mapper<Integer, PriorityDispatchContext>() {
        public Integer map(PriorityDispatchContext element) {
            return element.listPrio;
        }
    };

    protected PriorityDispatcher(String name, int priorities, PooledDispatcher<D> pooledDispactcher) {
        this.name = name;
        MAX_USER_PRIORITY = priorities;
        priorityQueue = new PriorityLinkedList<PriorityDispatchContext>(MAX_USER_PRIORITY + 1, PRIORITY_MAPPER);
        foreignQueue = createForeignEventQueue();
        for (int i = 0; i < 2; i++) {
            foreignQueue[i] = new LinkedNodeList<ForeignEvent>();
        }
        this.pooledDispatcher = pooledDispactcher;
    }

    public static final IDispatcher createPriorityDispatcher(String name, int numPriorities) {
        return new PriorityDispatcher(name, numPriorities, null);
    }

    public static final IDispatcher createPriorityDispatchPool(String name, final int numPriorities, int size) {
        return new AbstractPooledDispatcher<PriorityDispatcher>(name, size) {

            @Override
            protected final PriorityDispatcher createDispatcher(String name, AbstractPooledDispatcher<PriorityDispatcher> pool) throws Exception {
                // TODO Auto-generated method stub
                return new PriorityDispatcher(name, numPriorities, this);
            }

            public final Executor createPriorityExecutor(final int priority) {
                return new Executor() {
                    public void execute(final Runnable runnable) {
                        chooseDispatcher().dispatch(new RunnableAdapter(runnable), priority);
                    }
                };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private LinkedNodeList<ForeignEvent>[] createForeignEventQueue() {
        return new LinkedNodeList[2];
    }

    protected abstract class ForeignEvent extends LinkedNode<ForeignEvent> {
        public abstract void execute();

        final void addToList() {
            synchronized (foreignQueue) {
                if (!this.isLinked()) {
                    foreignQueue[foreignToggle].addLast(this);
                    if (!foreignAvailable.getAndSet(true)) {
                        wakeup();
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
            pdc.processForeignUpdates();
        }
    }

    public DispatchContext register(Dispatchable dispatchable, String name) {
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
    public synchronized void shutdown() throws InterruptedException {
        if (thread != null) {
            dispatch(new RunnableAdapter() {
                public void run() {
                    running = false;
                }
            }, MAX_USER_PRIORITY + 1);
            // thread.interrupt();
            thread.join();
            thread = null;
        }
    }

    public void run() {

        // Inform the dispatcher that we have started:
        pooledDispatcher.onDispatcherStarted((D) this);
        PriorityDispatchContext pdc;
        try {
            while (running) {
                pdc = priorityQueue.poll();
                // If no local work available wait for foreign work:
                if (pdc == null) {
                    waitForEvents();
                } else {
                    if (pdc.tracker != null) {
                        pooledDispatcher.setCurrentDispatchContext(pdc);
                    }

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

                    pooledDispatcher.setCurrentDispatchContext(null);
                }

                // Execute delayed events:
                timerHeap.executeReadyTimers();

                // Allow subclasses to do additional work:
                dispatchHook();

                // Check for foreign dispatch requests:
                if (foreignAvailable.get()) {
                    LinkedNodeList<ForeignEvent> foreign;
                    synchronized (foreignQueue) {
                        // Swap foreign queues and drain permits;
                        foreign = foreignQueue[foreignToggle];
                        foreignToggle = TOGGLE[foreignToggle];
                        foreignAvailable.set(false);
                        foreignPermits.drainPermits();
                    }
                    while (true) {
                        ForeignEvent fe = foreign.getHead();
                        if (fe == null) {
                            break;
                        }

                        fe.unlink();
                        fe.execute();
                    }

                }
            }
        } catch (InterruptedException e) {
            return;
        } catch (Throwable thrown) {
            thrown.printStackTrace();
        } finally {
            pooledDispatcher.onDispatcherStopped((D) this);
        }
    }

    /**
     * Subclasses may override this to do do additional dispatch work:
     */
    protected void dispatchHook() throws Exception {

    }

    /**
     * Subclasses may override this to implement another mechanism for wakeup.
     * 
     * @throws Exception
     */
    protected void waitForEvents() throws Exception {
        foreignPermits.acquire();
    }

    /**
     * Subclasses may override this to provide an alternative wakeup mechanism.
     */
    protected void wakeup() {
        foreignPermits.release();
    }

    protected final void onForeignUdate(PriorityDispatchContext context) {
        synchronized (foreignQueue) {

            ForeignEvent fe = context.updateEvent[foreignToggle];
            if (!fe.isLinked()) {
                foreignQueue[foreignToggle].addLast(fe);
                if (!foreignAvailable.getAndSet(true)) {
                    wakeup();
                }
            }
        }
    }

    protected final boolean removeDispatchContext(PriorityDispatchContext context) {
        synchronized (foreignQueue) {

            if (context.updateEvent[0].isLinked()) {
                context.updateEvent[0].unlink();
            }
            if (context.updateEvent[1].isLinked()) {
                context.updateEvent[1].unlink();
            }
            if (context.isLinked()) {
                context.unlink();
                return true;
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.dispatch.IDispatcher#dispatch(org.apache.activemq
     * .dispatch.Dispatcher.Dispatchable)
     */
    public final void dispatch(Dispatchable dispatchable, int priority) {
        PriorityDispatchContext context = new PriorityDispatchContext(dispatchable, false, name);
        context.updatePriority(priority);
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
        if (getCurrentDispatcher() == this) {
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

    private final D getCurrentDispatcher() {
        return pooledDispatcher.getCurrentDispatcher();
    }

    private final PooledDispatchContext<D> getCurrentDispatchContext() {
        return pooledDispatcher.getCurrentDispatchContext();
    }

    /**
     * 
     */
    protected class PriorityDispatchContext extends LinkedNode<PriorityDispatchContext> implements PooledDispatchContext<D> {
        // The dispatchable target:
        private final Dispatchable dispatchable;
        // The name of this context:
        final String name;
        // list prio can only be updated in the thread of of the owning
        // dispatcher
        protected int listPrio;

        // The update events are used to update fields in the dispatch context
        // from foreign threads:
        final UpdateEvent updateEvent[];

        private final ExecutionTracker<D> tracker;
        protected D currentOwner;
        private D updateDispatcher = null;

        private int priority;
        private boolean dispatchRequested = false;
        private boolean closed = false;

        protected PriorityDispatchContext(Dispatchable dispatchable, boolean persistent, String name) {
            this.dispatchable = dispatchable;
            this.name = name;
            this.currentOwner = (D) PriorityDispatcher.this;
            if (persistent) {
                this.tracker = pooledDispatcher.getLoadBalancer().createExecutionTracker((PooledDispatchContext<D>) this);
            } else {
                this.tracker = null;
            }
            updateEvent = createUpdateEvent();
            updateEvent[0] = new UpdateEvent(this);
            updateEvent[1] = new UpdateEvent(this);
        }

        @SuppressWarnings("unchecked")
        private final PriorityDispatcher<D>.UpdateEvent[] createUpdateEvent() {
            return new PriorityDispatcher.UpdateEvent[2];
        }

        /**
         * Gets the execution tracker for the context.
         * 
         * @return the execution tracker for the context:
         */
        public ExecutionTracker<D> getExecutionTracker() {
            return tracker;
        }

        /**
         * This can only be called by the owning dispatch thread:
         * 
         * @return False if the dispatchable has more work to do.
         */
        public final boolean dispatch() {
            return dispatchable.dispatch();
        }

        public final void assignToNewDispatcher(D newDispatcher) {
            synchronized (this) {

                // If we're already set to this dispatcher
                if (newDispatcher == currentOwner) {
                    if (updateDispatcher == null || updateDispatcher == newDispatcher) {
                        return;
                    }
                }

                updateDispatcher = newDispatcher;
                if (DEBUG)
                    System.out.println(getName() + " updating to " + updateDispatcher);
            }
            currentOwner.onForeignUdate(this);
        }

        public void requestDispatch() {

            if (closed) {
                throw new RejectedExecutionException();
            }
            D callingDispatcher = getCurrentDispatcher();
            if (tracker != null)
                tracker.onDispatchRequest(callingDispatcher, getCurrentDispatchContext());

            // Otherwise this is coming off another thread, so we need to
            // synchronize
            // to protect against ownership changes:
            synchronized (this) {
                // If the owner of this context is the calling thread, then
                // delegate to the dispatcher.
                if (currentOwner == callingDispatcher) {

                    if (!isLinked()) {
                        currentOwner.priorityQueue.add(this, listPrio);
                    }
                    return;
                }

                dispatchRequested = true;
            }
            // FIXME Thread safety!
            currentOwner.onForeignUdate(this);
        }

        public void updatePriority(int priority) {
            if (this.priority == priority) {
                return;
            }
            D callingDispatcher = getCurrentDispatcher();

            // Otherwise this is coming off another thread, so we need to
            // synchronize to protect against ownership changes:
            synchronized (this) {
                this.priority = priority;

                // If this is called by the owning dispatcher, then we go ahead
                // and update:
                if (currentOwner == callingDispatcher) {

                    if (priority != listPrio) {

                        listPrio = priority;
                        // If there is a priority change relink the context
                        // at the new priority:
                        if (isLinked()) {
                            unlink();
                            currentOwner.priorityQueue.add(this, listPrio);
                        }
                    }
                    return;
                }
            }
            // FIXME Thread safety!
            currentOwner.onForeignUdate(this);
        }

        public void processForeignUpdates() {
            boolean ownerChange = false;
            synchronized (this) {

                if (closed) {
                    close();
                    return;
                }

                if (updateDispatcher != null) {
                    if (DEBUG) {
                        System.out.println("Assigning " + getName() + " to " + updateDispatcher);
                    }
                    if (currentOwner.removeDispatchContext(this)) {
                        dispatchRequested = true;
                    }
                    currentOwner = updateDispatcher;
                    updateDispatcher = null;
                    ownerChange = true;
                } else {
                    updatePriority(priority);

                    if (dispatchRequested) {
                        dispatchRequested = false;
                        requestDispatch();
                    }
                }
            }

            if (ownerChange) {
                currentOwner.onForeignUdate(this);
            }
        }

        /**
         * May be overriden by subclass to additional work on dispatcher switch
         * 
         * @param oldDispatcher
         *            The old dispatcher
         * @param newDispatcher
         *            The new Dispatcher
         */
        protected void switchedDispatcher(D oldDispatcher, D newDispatcher) {

        }

        public void close() {
            D callingDispatcher = getCurrentDispatcher();
            synchronized (this) {
                closed = true;
                // If the owner of this context is the calling thread, then
                // delegate to the dispatcher.
                if (currentOwner == callingDispatcher) {
                    if (isLinked()) {
                        unlink();
                    }
                    tracker.close();

                    // FIXME Deadlock potential!
                    synchronized (foreignQueue) {
                        if (updateEvent[foreignToggle].isLinked()) {
                            updateEvent[foreignToggle].unlink();
                        }
                    }
                }
            }
            currentOwner.onForeignUdate(this);
        }

        public final String toString() {
            return getName();
        }

        public Dispatchable getDispatchable() {
            return dispatchable;
        }

        public D getDispatcher() {
            return currentOwner;
        }

        public String getName() {
            return name;
        }
    }
}
