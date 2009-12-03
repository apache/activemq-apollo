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
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.internal.advanced.ExecutionLoadBalancer.ExecutionTracker;
import org.apache.activemq.dispatch.internal.advanced.PooledDispatcher.PooledDispatchContext;
import org.apache.activemq.util.Mapper;
import org.apache.activemq.util.PriorityLinkedList;
import org.apache.activemq.util.TimerHeap;
import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;

public class PriorityDispatcher implements Runnable, IDispatcher {

    private static final boolean DEBUG = false;
    private Thread thread;
    protected boolean running = false;
    private boolean threaded = false;
    protected final int MAX_USER_PRIORITY;
    protected final HashSet<PriorityDispatchContext> contexts = new HashSet<PriorityDispatchContext>();

    // Set if this dispatcher is part of a dispatch pool:
    protected final PooledDispatcher pooledDispatcher;

    // The local dispatch queue:
    protected final PriorityLinkedList<PriorityDispatchContext> priorityQueue;

    // Dispatch queue for requests from other threads:
    private final LinkedNodeList<ForeignEvent>[] foreignQueue;
    private static final int[] TOGGLE = new int[] { 1, 0 };
    private int foreignToggle = 0;

    // Timed Execution List
    protected final TimerHeap<Runnable> timerHeap = new TimerHeap<Runnable>() {
        @Override
        protected final void execute(Runnable ready) {
            ready.run();
        }
    };

    protected final String name;
    private final AtomicBoolean foreignAvailable = new AtomicBoolean(false);
    private final Semaphore foreignPermits = new Semaphore(0);

    private final Mapper<Integer, PriorityDispatchContext> PRIORITY_MAPPER = new Mapper<Integer, PriorityDispatchContext>() {
        public Integer map(PriorityDispatchContext element) {
            return element.listPrio;
        }
    };

    protected PriorityDispatcher(String name, int priorities, PooledDispatcher pooledDispactcher) {
        this.name = name;
        MAX_USER_PRIORITY = priorities - 1;
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
        return new PooledPriorityDispatcher(name, size, numPriorities);
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

    public int getDispatchPriorities() {
        return MAX_USER_PRIORITY;
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
    public void shutdown() throws InterruptedException {
        Thread joinThread = null;
        synchronized (this) {
            if (thread != null) {
                dispatchInternal(new RunnableAdapter() {
                    public void run() {
                        running = false;
                    }
                }, MAX_USER_PRIORITY + 1);
                joinThread = thread;
                thread = null;
            }
        }
        if (joinThread != null) {
            // thread.interrupt();
            joinThread.join();
        }
    }

    protected void cleanup() {
        ArrayList<PriorityDispatchContext> toClose = null;
        synchronized (this) {
            running = false;
            toClose = new ArrayList<PriorityDispatchContext>(contexts.size());
            toClose.addAll(contexts);
        }

        for (PriorityDispatchContext context : toClose) {
            context.close(false);
        }
    }

    public void run() {

        if (pooledDispatcher != null) {
            // Inform the dispatcher that we have started:
            pooledDispatcher.onDispatcherStarted((PriorityDispatcher) this);
        }

        PriorityDispatchContext pdc;
        try {
            final int MAX_DISPATCH_PER_LOOP = 20;
            int processed = 0;

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
                        processed++;
                        if (processed > MAX_DISPATCH_PER_LOOP || pdc.listPrio < priorityQueue.getHighestPriority()) {
                            // Give other dispatchables a shot:
                            // May have gotten relinked by the caller:
                            if (!pdc.isLinked()) {
                                priorityQueue.add(pdc, pdc.listPrio);
                            }
                            break;
                        }
                    }

                    if (pdc.tracker != null) {
                        pooledDispatcher.setCurrentDispatchContext(null);
                    }

                    if (processed < MAX_DISPATCH_PER_LOOP) {
                        continue;
                    }
                }

                processed = 0;
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
            if (pooledDispatcher != null) {
                pooledDispatcher.onDispatcherStopped((PriorityDispatcher) this);
            }
            cleanup();
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
        long next = timerHeap.timeToNext(TimeUnit.NANOSECONDS);
        if (next == -1) {
            foreignPermits.acquire();
        } else if (next > 0) {
            foreignPermits.tryAcquire(next, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Subclasses may override this to provide an alternative wakeup mechanism.
     */
    protected void wakeup() {
        foreignPermits.release();
    }

    protected final void onForeignUpdate(PriorityDispatchContext context) {
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
        }

        if (context.isLinked()) {
            context.unlink();
            return true;
        }

        synchronized (this) {
            contexts.remove(context);
        }

        return false;
    }

    protected final boolean takeOwnership(PriorityDispatchContext context) {
        synchronized (this) {
            if (running) {
                contexts.add(context);
            } else {
                return false;
            }
        }
        return true;
    }

    //Special dispatch method that allow high priority dispatch:
    private final void dispatchInternal(Dispatchable dispatchable, int priority) {
        PriorityDispatchContext context = new PriorityDispatchContext(dispatchable, false, name);
        context.priority = priority;
        context.requestDispatch();
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
            timerHeap.addRelative(runnable, delay, timeUnit);
        } else {
            new ForeignEvent() {
                public void execute() {
                    timerHeap.addRelative(runnable, delay, timeUnit);
                }
            }.addToList();
        }
    }

    public String toString() {
        return name;
    }

    private final PriorityDispatcher getCurrentDispatcher() {
        if (pooledDispatcher != null) {
            return (PriorityDispatcher) pooledDispatcher.getCurrentDispatcher();
        } else if (Thread.currentThread() == thread) {
            return (PriorityDispatcher) this;
        } else {
            return null;
        }

    }

    private final PooledDispatchContext getCurrentDispatchContext() {
        return pooledDispatcher.getCurrentDispatchContext();
    }

    /**
     * 
     */
    protected class PriorityDispatchContext extends LinkedNode<PriorityDispatchContext> implements PooledDispatchContext {
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

        private final ExecutionTracker tracker;
        protected PriorityDispatcher currentOwner;
        private PriorityDispatcher updateDispatcher = null;

        private int priority;
        private boolean dispatchRequested = false;
        private boolean closed = false;
        final CountDownLatch closeLatch = new CountDownLatch(1);

        protected PriorityDispatchContext(Dispatchable dispatchable, boolean persistent, String name) {
            this.dispatchable = dispatchable;
            this.name = name;
            this.currentOwner = (PriorityDispatcher) PriorityDispatcher.this;
            if (persistent && pooledDispatcher != null) {
                this.tracker = pooledDispatcher.getLoadBalancer().createExecutionTracker((PooledDispatchContext) this);
            } else {
                this.tracker = null;
            }
            updateEvent = createUpdateEvent();
            updateEvent[0] = new UpdateEvent(this);
            updateEvent[1] = new UpdateEvent(this);
            if (persistent) {
                currentOwner.takeOwnership(this);
            }
        }

        private final PriorityDispatcher.UpdateEvent[] createUpdateEvent() {
            return new PriorityDispatcher.UpdateEvent[2];
        }

        /**
         * Gets the execution tracker for the context.
         * 
         * @return the execution tracker for the context:
         */
        public ExecutionTracker getExecutionTracker() {
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

        public final void assignToNewDispatcher(IDispatcher newDispatcher) {
            synchronized (this) {

                // If we're already set to this dispatcher
                if (newDispatcher == currentOwner) {
                    if (updateDispatcher == null || updateDispatcher == newDispatcher) {
                        return;
                    }
                }

                updateDispatcher = (PriorityDispatcher) newDispatcher;
                if (DEBUG)
                    System.out.println(getName() + " updating to " + updateDispatcher);

                currentOwner.onForeignUpdate(this);
            }

        }

        public void requestDispatch() {

            PriorityDispatcher callingDispatcher = getCurrentDispatcher();
            if (tracker != null)
                tracker.onDispatchRequest(callingDispatcher, getCurrentDispatchContext());

            // Otherwise this is coming off another thread, so we need to
            // synchronize
            // to protect against ownership changes:
            synchronized (this) {
                // If the owner of this context is the calling thread, then
                // delegate to the dispatcher.
                if (currentOwner == callingDispatcher) {

                    if (!currentOwner.running) {
                        // TODO In the event that the current dispatcher
                        // failed due to a runtime exception, we could
                        // try to switch to a new dispatcher.
                        throw new RejectedExecutionException();
                    }
                    if (!isLinked()) {
                        currentOwner.priorityQueue.add(this, listPrio);
                    }
                    return;
                }

                dispatchRequested = true;
                currentOwner.onForeignUpdate(this);
            }
        }

        public void updatePriority(int priority) {

            if (closed) {
                return;
            }

            priority = Math.min(priority, MAX_USER_PRIORITY);

            if (this.priority == priority) {
                return;
            }
            PriorityDispatcher callingDispatcher = getCurrentDispatcher();

            // Otherwise this is coming off another thread, so we need to
            // synchronize to protect against ownership changes:
            synchronized (this) {
                if (closed) {
                    return;
                }
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

                currentOwner.onForeignUpdate(this);
            }

        }

        public void processForeignUpdates() {
            synchronized (this) {

                if (closed) {
                    close(false);
                    return;
                }

                if (updateDispatcher != null && updateDispatcher.takeOwnership(this)) {
                    if (DEBUG) {
                        System.out.println("Assigning " + getName() + " to " + updateDispatcher);
                    }

                    if (currentOwner.removeDispatchContext(this)) {
                        dispatchRequested = true;
                    }

                    updateDispatcher.onForeignUpdate(this);
                    switchedDispatcher(currentOwner, updateDispatcher);
                    currentOwner = updateDispatcher;
                    updateDispatcher = null;

                } else {
                    updatePriority(priority);

                    if (dispatchRequested) {
                        dispatchRequested = false;
                        requestDispatch();
                    }
                }
            }
        }

        /**
         * May be overriden by subclass to additional work on dispatcher switch
         * 
         * @param oldDispatcher The old dispatcher
         * @param newDispatcher The new Dispatcher
         */
        protected void switchedDispatcher(PriorityDispatcher oldDispatcher, PriorityDispatcher newDispatcher) {

        }

        public boolean isClosed() {
            return closed;
        }

        public void close(boolean sync) {
            PriorityDispatcher callingDispatcher = getCurrentDispatcher();
            // System.out.println(this + "Closing");
            synchronized (this) {
                closed = true;
                // If the owner of this context is the calling thread, then
                // delegate to the dispatcher.
                if (currentOwner == callingDispatcher) {
                    removeDispatchContext(this);
                    closeLatch.countDown();
                    return;
                }
            }

            currentOwner.onForeignUpdate(this);
            if (sync) {
                boolean interrupted = false;
                while (true) {
                    try {
                        closeLatch.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }

                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public final String toString() {
            return getName();
        }

        public Dispatchable getDispatchable() {
            return dispatchable;
        }

        public PriorityDispatcher getDispatcher() {
            return currentOwner;
        }

        public String getName() {
            return name;
        }

    }

    public String getName() {
        return name;
    }
}
