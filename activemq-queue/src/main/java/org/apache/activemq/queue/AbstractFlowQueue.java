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
package org.apache.activemq.queue;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.dispatch.IDispatcher.Dispatchable;
import org.apache.activemq.flow.ISinkController.FlowControllable;

/**
 * Base class for a {@link Dispatchable} {@link FlowControllable}
 * {@link IFlowQueue}.
 * 
 * @param <E>
 */
public abstract class AbstractFlowQueue<E> extends AbstractFlowRelay<E> implements FlowControllable<E>, IFlowQueue<E>, Dispatchable {

    protected IDispatcher dispatcher;
    protected DispatchContext dispatchContext;
    protected Collection<IPollableFlowSource.FlowReadyListener<E>> readyListeners;
    private boolean notifyReady = false;
    protected int dispatchPriority = 0;
    protected FlowQueueListener listener = new FlowQueueListener() {
        public void onQueueException(IFlowQueue<?> queue, Throwable thrown) {
            System.out.println("Exception in queue: " + thrown.getMessage());
            thrown.printStackTrace();
        }
    };
    protected boolean started;
    protected Subscription<E> sub;

    AbstractFlowQueue() {
        super();
    }

    protected AbstractFlowQueue(String name) {
        super(name);
    }

    public void setFlowQueueListener(FlowQueueListener listener) {
        this.listener = listener;
    }

    public final boolean dispatch() {

        // while (pollingDispatch());
        // return true;

        return !pollingDispatch();
    }

    protected final FlowControllable<E> getFlowControllableHook() {
        return this;
    }

    public synchronized void start() {
        if (!started) {
            started = true;
            if (isDispatchReady()) {
                notifyReady();
            }
        }
    }

    public synchronized void stop() {
        started = false;
    }

    /**
     * Calls stop and cleans up resources associated with the queue.
     * 
     * @param sync
     */
    public void shutdown(boolean sync) {
        stop();
        DispatchContext dc = null;
        synchronized (this) {
            dc = dispatchContext;
            dispatchContext = null;

        }

        if (dc != null) {
            dc.close(sync);
        }
    }

    /**
     * Adds a subscription to the queue. When the queue is started and elements
     * are available, they will be given to the subscription.
     * 
     * @param sub
     *            The subscription to add to the queue.
     */
    public synchronized void addSubscription(Subscription<E> sub) {
        if (sub == null) {
            this.sub = sub;
        } else if (sub != sub) {
            //TODO change this to a checked exception.
            throw new IllegalStateException("Sub already connected");
        }
    }

    /**
     * Removes a subscription from the queue.
     * 
     * @param sub
     *            The subscription to remove.
     */
    public synchronized boolean removeSubscription(Subscription<E> sub) {
        if (this.sub == sub) {
            sub = null;
            return true;
        }
        return false;
    }

    /**
     * Sets an asynchronous dispatcher for this source. As elements become
     * available they will be dispatched to the worker pool.
     * 
     * @param workers
     *            The executor thread pool.
     * @param dispatcher
     *            The dispatcher to handle messages.
     */
    public synchronized void setDispatcher(IDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        dispatchContext = dispatcher.register(this, getResourceName());
        dispatchContext.updatePriority(dispatchPriority);
        super.setFlowExecutor(dispatcher.createPriorityExecutor(dispatcher.getDispatchPriorities() - 1));
    }

    public synchronized void setDispatchPriority(int priority) {
        dispatchPriority = priority;
        if (dispatchContext != null) {
            dispatchContext.updatePriority(priority);
        }
    }

    public synchronized void addFlowReadyListener(IPollableFlowSource.FlowReadyListener<E> watcher) {

        if (readyListeners == null) {
            readyListeners = new ArrayList<IPollableFlowSource.FlowReadyListener<E>>();
        }
        readyListeners.add(watcher);
        if (isDispatchReady()) {
            notifyReady();
        }
    }

    /**
     * Dispatches an element potentialy blocking until an element is available
     * for dispatch.
     */
    public final void blockingDispatch() throws InterruptedException {

        while (!pollingDispatch()) {
            waitForDispatchReady();
        }
    }

    /**
     * Indicates that there are elements ready for dispatch.
     */
    protected void notifyReady() {
        if (dispatchContext != null) {
            dispatchContext.requestDispatch();
            return;
        }

        synchronized (this) {

            if (notifyReady) {
                notify();
            }

            if (readyListeners == null) {
                return;
            }

            if (!readyListeners.isEmpty()) {
                for (FlowReadyListener<E> listener : readyListeners) {
                    listener.onFlowReady(this);
                }
            }

            readyListeners.clear();
        }
    }

    protected synchronized void waitForDispatchReady() throws InterruptedException {
        while (!isDispatchReady()) {
            notifyReady = true;
            wait();
        }
        notifyReady = false;
    }

    public String toString() {
        return getResourceName();
    }
}
