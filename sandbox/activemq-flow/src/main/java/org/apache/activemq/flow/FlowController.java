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
package org.apache.activemq.flow;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.apache.activemq.flow.IFlowLimiter.UnThrottleListener;
import org.apache.activemq.util.IntrospectionSupport;

/**
 */
public class FlowController<E> implements IFlowController<E> {

    private static final Executor DEFAULT_EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    
    // Sinks that are blocking us.
    private final HashSet<ISinkController<?>> blockingSinks = new HashSet<ISinkController<?>>();

    // Holds the sources that this limiter is currently blocking
    private final HashSet<ISourceController<?>> blockedSources = new HashSet<ISourceController<?>>();

    // Holds the sources that this limiter is currently blocking
    private final HashSet<FlowUnblockListener<E>> unblockListeners = new HashSet<FlowUnblockListener<E>>();

    // Callback for the IFlowLimiter to notify us that it is unthrottled:
    private final UnThrottleListener unthrottleListener;

    // The flow being flow controlled:
    protected Flow flow;

    // Used to synchronize access to this controller by downstream sinks:
    // private final ReentrantLock sinkLock = new
    // java.util.concurrent.locks.ReentrantLock();

    // True if the flow is blocked:
    protected boolean blocked = false;

    // Set to true while resuming. New Sources must wait to enqueue while
    // old sources are being resumed.
    private boolean resuming = false;

    // Marks that we have scheduled a resume
    private boolean resumeScheduled = false;

    // The acceptor for elements from this flow.
    private FlowControllable<E> controllable;

    // The limiter
    private IFlowLimiter<E> limiter;

    // Mutex for synchronization
    private Object mutex;

    private boolean useOverFlowQueue = true;

    // List of elements that were added while the flow is blocked.
    // These aren't added to the resource until the flow becomes
    // unblocked.
    private LinkedList<E> overflowQueue = new LinkedList<E>();

    // true we registered as an unthrottle listener:
    private boolean throttleReg;
    private boolean notifyUnblock = false;
    private String name;
    private Executor executor = DEFAULT_EXECUTOR;

    public FlowController() {
        this.unthrottleListener = new UnThrottleListener() {
            public final void onUnthrottled() {
                FlowController.this.onUnthrottled();
            }
            @Override
            public String toString() {
                return "DEFAULT";
            }
        };
    }

    public FlowController(FlowControllable<E> controllable, Flow flow, IFlowLimiter<E> limiter, Object mutex) {
        this();
        this.controllable = controllable;
        this.flow = flow;
        this.limiter = limiter == null ? new SizeLimiter<E>(0, 0) : limiter;
        this.mutex = mutex;
        if(controllable != null)
        {
            this.name = controllable.toString();
        }
    }

    public String toString() {
        return IntrospectionSupport.toString(this);
    }

    public final IFlowLimiter<E> getLimiter() {
        return limiter;
    }

    public final void setLimiter(IFlowLimiter<E> limiter) {
        synchronized (mutex) {
            this.limiter = limiter;
            onUnthrottled();
        }
    }

    /**
     * Sets whether the controller uses an overflow queue to prevent overflow.
     */
    public final void useOverFlowQueue(boolean val) {
        useOverFlowQueue = val;
    }

    public final Flow getFlow() {
        return flow;
    }

    /**
     * Should be called by a resource anytime it's limits are exceeded.
     */
    public final void onFlowBlock(ISinkController<?> sinkController) {
        synchronized (mutex) {
            if (!blockingSinks.add(sinkController)) {
                throw new IllegalStateException(sinkController + " has already blocked: " + this);
            }

            if (!blocked) {
                blocked = true;
                // System.out.println(this + " BLOCKED");
            }
        }
    }

    public final void onFlowResume(ISinkController<?> sinkController) {
        synchronized (mutex) {
            if (!blockingSinks.remove(sinkController)) {
                throw new IllegalStateException(sinkController + " can't resume unblocked " + this);
            }

            if (blockingSinks.isEmpty()) {
                if (blocked) {
                    blocked = false;
                    limiter.releaseReserved();
                }
            }
        }
    }

    /**
     * Must be called once the elements have been sent to downstream sinks.
     * 
     * @param elem
     *            The dispatched element.
     * @return
     */
    public final void elementDispatched(E elem) {

        synchronized (mutex) {
            // If we were blocked in the course of dispatching the message don't
            // decrement
            // the limiter space:
            if (blocked) {
                limiter.reserve(elem);
                return;
            }
            limiter.remove(elem);
        }
    }

    public final boolean isSinkBlocked() {
        synchronized (mutex) {
            return limiter.getThrottled();
        }
    }

    /**
     * Waits for a flow to become unblocked.
     * 
     * @param flow
     *            The flow.
     * @throws InterruptedException
     *             If interrupted while waiting.
     */
    public void waitForFlowUnblock() throws InterruptedException {
        synchronized (mutex) {
            while (limiter.getThrottled()) {
                notifyUnblock = true;
                setUnThrottleListener();
                mutex.wait();
            }
            notifyUnblock = false;
        }
    }

    public IFlowResource getFlowResource() {
        if (controllable != null) {
            return controllable.getFlowResource();
        } else {
            return null;
        }
    }

    /**
     * Adds an element to the sink associated with this resource if space is
     * available. If no space is available the source controller will be
     * blocked, and the source is responsible for tracking the space until this
     * controller resumes.
     * 
     * @param elem
     *            The element to add.
     * @param controller
     *            the source flow controller.
     */
    public void add(E elem, ISourceController<?> sourceController) {
        boolean ok = false;
        synchronized (mutex) {
            if (okToAdd(elem)) {
                ok = true;
                if (limiter.add(elem)) {
                    blockSource(sourceController);
                }
            } else {
                // Add to overflow queue and restoreBlock source:
                overflowQueue.add(elem);
                if (sourceController != null) {
                    blockSource(sourceController);
                } else if (!resuming) {
                    setUnThrottleListener();
                }
            }
        }
        if (ok && controllable != null) {
            controllable.flowElemAccepted(this, elem);
        }
    }

    /**
     * Offers an element to the sink associated with this resource if space is
     * available. If no space is available false is returned. The element does
     * not get added to the overflow list.
     * 
     * @param elem
     *            The element to add.
     * @param controller
     *            the source flow controller.
     */
    public boolean offer(E elem, ISourceController<?> sourceController) {
        boolean ok = false;
        synchronized (mutex) {
            if (okToAdd(elem)) {
                if (limiter.add(elem)) {
                    blockSource(sourceController);
                }
                ok = true;
            } else {
                blockSource(sourceController);
            }
        }
        if (ok && controllable != null) {
            controllable.flowElemAccepted(this, elem);
        }
        return ok;
    }

    private boolean okToAdd(E elem) {
        return !useOverFlowQueue || limiter.canAdd(elem);
    }

    private void addToResource(E elem) {
        if (limiter != null)
            limiter.add(elem);
        if (controllable != null)
            controllable.flowElemAccepted(this, elem);
    }

    private void setUnThrottleListener() {
        if (!throttleReg) {
            throttleReg = true;
            limiter.addUnThrottleListener(unthrottleListener);
        }
    }

    public void onUnthrottled() {
        synchronized (mutex) {
            throttleReg = false;
            dispatchOverflowQueue();
        }
    }

    /**
     * Blocks a source.
     * 
     * @param source
     *            The {@link ISinkController} of the source to be blocked.
     */
    protected void blockSource(final ISourceController<?> source) {
        if (source == null) {
            return;
        }

        // TODO This could allow a single source to completely overflow the
        // queue
        // during resume
        if (resuming) {
            return;
        }

        setUnThrottleListener();

        if (!blockedSources.contains(source)) {
            // System.out.println("BLOCKING  : SINK[" + this + "], SOURCE[" +
            // source + "]");
            blockedSources.add(source);
            source.onFlowBlock(this);
        }
    }

    private void dispatchOverflowQueue() {

        // Dispatch elements on the blocked list into the limited resource
        while (!overflowQueue.isEmpty()) {
            E elem = overflowQueue.getFirst();
            if (limiter.canAdd(elem)) {
                overflowQueue.removeFirst();
                addToResource(elem);
            } else {
                break;
            }
        }

        // See if we can now unblock the sources.
        checkUnblockSources();

        // If we've exceeded the the throttle threshold, register
        // a listener so we can resume the blocked sources after
        // the limiter falls below the threshold:
        if (!overflowQueue.isEmpty() || limiter.getThrottled()) {
            setUnThrottleListener();
        } else if (notifyUnblock) {
            mutex.notifyAll();
        }
    }

    /**
     * Called to wait for a flow to become unblocked.
     * 
     * @param listener
     *            The listener.
     * @return true;
     */
    public final boolean addUnblockListener(FlowUnblockListener<E> listener) {
        synchronized (mutex) {
            if (!resuming && (limiter.getThrottled() || !overflowQueue.isEmpty())) {
                setUnThrottleListener();
                unblockListeners.add(listener);
                return true;
            }
        }
        return false;
    }

    /**
     * Releases blocked sources providing the limiter isn't throttled, and there
     * are no elements on the blocked list.
     */
    private void checkUnblockSources() {
        if (!resumeScheduled && !limiter.getThrottled() && overflowQueue.isEmpty() && (!blockedSources.isEmpty() || !unblockListeners.isEmpty())) {
            resumeScheduled = true;
            Runnable resume = new Runnable() {
                public void run() {
                    synchronized (mutex) {
                        resuming = true;
                    }
                    try {
                        for (ISourceController<?> source : blockedSources) {
                            // System.out.println("UNBLOCKING: SINK[" +
                            // FlowController.this + "], SOURCE[" + source +
                            // "]");
                            source.onFlowResume(FlowController.this);
                        }
                        for (FlowUnblockListener<E> listener : unblockListeners) {
                            // System.out.println(this + "Unblocking source " +
                            // source );
                            listener.onFlowUnblocked(FlowController.this);
                        }

                    } finally {
                        synchronized (mutex) {
                            blockedSources.clear();
                            unblockListeners.clear();
                            resuming = false;
                            resumeScheduled = false;
                            mutex.notifyAll();
                        }
                    }
                }
            };

            try {
                executor.execute(resume);
            } catch (RejectedExecutionException ree) {
                // Must be shutting down, ignore this, leaving resumeScheduled
                // true
            }
        }
    }

    public void setExecutor(Executor executor) {
        synchronized (mutex) {
            this.executor = executor;
        }

    }
}
