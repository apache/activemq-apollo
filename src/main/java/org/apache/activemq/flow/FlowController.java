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

import org.apache.activemq.flow.IFlowLimiter.UnThrottleListener;

/**
 */
public class FlowController<E> implements ISinkController<E>, ISourceController<E> {

    // Sinks that are blocking us.
    private final HashSet<ISinkController<E>> blockingSinks = new HashSet<ISinkController<E>>();

    // Holds the sources that this limiter is currently blocking
    private final HashSet<ISourceController<E>> blockedSources = new HashSet<ISourceController<E>>();

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

    public FlowController() {
        this.unthrottleListener = new UnThrottleListener() {
            public final void onUnthrottled() {
                FlowController.this.onUnthrottled();
            }
        };
    }

    public FlowController(FlowControllable<E> controllable, Flow flow, IFlowLimiter<E> limiter, Object mutex) {
        this();
        this.controllable = controllable;
        this.flow = flow;
        this.limiter = limiter == null ? new SizeLimiter<E>(0, 0) : limiter;
        this.mutex = mutex;
        this.name = controllable.toString();
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
    public final void onFlowBlock(ISinkController<E> sinkController) {
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

    public final void onFlowResume(ISinkController<E> sinkController) {
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

    public final boolean isSourceBlocked() {
        synchronized (mutex) {
            return blocked;
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

    public IFlowSource<E> getFlowSource() {
        return controllable.getFlowSource();
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
    public void add(E elem, ISourceController<E> sourceController) {
        boolean ok = false;
        synchronized (mutex) {
            // If we don't have an fc sink, then just increment the limiter.
            if (controllable == null) {
                limiter.add(elem);
                return;
            }
            if (okToAdd(elem)) {
                ok = true;
                if (limiter.add(elem)) {
                    setUnThrottleListener();
                    blockSource(sourceController);
                }
            } else {
                // Add to overflow queue and block source:
                overflowQueue.add(elem);
                setUnThrottleListener();
                blockSource(sourceController);
            }
        }
        if (ok) {
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
    public boolean offer(E elem, ISourceController<E> sourceController) {
        synchronized (mutex) {
            // If we don't have an fc sink, then just increment the limiter.
            if (controllable == null) {
                limiter.add(elem);
                return true;
            }

            if (okToAdd(elem)) {
                if (limiter.add(elem)) {
                    blockSource(sourceController);
                    setUnThrottleListener();
                }
                controllable.flowElemAccepted(this, elem);
                return true;
            } else {
                blockSource(sourceController);
                setUnThrottleListener();
                return false;
            }
        }
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
    protected void blockSource(final ISourceController<E> source) {
        if (source == null) {
            return;
        }

        // If we are currently in the process of resuming we
        // must wait for resume to complete, before we add to
        // the blocked list:
        waitForResume();

        if (!blockedSources.contains(source)) {
//            System.out.println("BLOCKING  : SINK[" + this + "], SOURCE[" + source + "]");
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
            waitForResume();
            if (limiter.getThrottled() || !overflowQueue.isEmpty()) {
                unblockListeners.add(listener);
                return true;
            }
        }
        return false;
    }

    private final void waitForResume() {
        boolean interrupted = false;
        while (resuming) {
            try {
                mutex.wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
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
                    String was = Thread.currentThread().getName();
                    try {
                        Thread.currentThread().setName(name);
                        for (ISourceController<E> source : blockedSources) {
//                            System.out.println("UNBLOCKING: SINK[" + FlowController.this + "], SOURCE[" + source + "]");
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
                        Thread.currentThread().setName(was);
                    }
                }
            };

            RESUME_SERVICE.execute(resume);
        }
    }

    private static Executor RESUME_SERVICE = Executors.newCachedThreadPool();

    public static final void setFlowExecutor(Executor executor) {
        RESUME_SERVICE = executor;
    }

    public String toString() {
        return name;
    }

    public IFlowSink<E> getFlowSink() {
        return controllable.getFlowSink();
    }
}
