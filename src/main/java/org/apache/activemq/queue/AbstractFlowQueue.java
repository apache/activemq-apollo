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
import org.apache.activemq.flow.AbstractLimitedFlowSource;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISinkController.FlowControllable;

/**
 * Base class for a {@link Dispatchable} {@link FlowControllable}
 * {@link IFlowQueue}.
 * 
 * @param <E>
 */
public abstract class AbstractFlowQueue<E> extends AbstractLimitedFlowSource<E> implements FlowControllable<E>, IFlowQueue<E>, Dispatchable {

    protected IDispatcher dispatcher;
    protected DispatchContext dispatchContext;
    protected final Collection<IPollableFlowSource.FlowReadyListener<E>> readyListeners = new ArrayList<IPollableFlowSource.FlowReadyListener<E>>();
    private boolean notifyReady = false;
    protected boolean dispatching = false;
    protected int dispatchPriority = 0;

    AbstractFlowQueue() {
        super();
    }

    protected AbstractFlowQueue(String name) {
        super(name);
    }

    public final boolean dispatch() {

        while (pollingDispatch())
            ;

        return true;

        // return !pollingDispatch();
    }

    public final IFlowSink<E> getFlowSink() {
        // TODO Auto-generated method stub
        return this;
    }

    public final IFlowSource<E> getFlowSource() {
        // TODO Auto-generated method stub
        return this;
    }

    protected final FlowControllable<E> getFlowControllableHook() {
        return this;
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
    }

    public synchronized final void setDispatchPriority(int priority) {
        dispatchPriority = priority;
        if (dispatchContext != null) {
            dispatchContext.updatePriority(priority);
        }
    }

    public synchronized void addFlowReadyListener(IPollableFlowSource.FlowReadyListener<E> watcher) {

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
            if (dispatchContext != null) {
                if (!dispatching) {
                    dispatching = true;
                    dispatchContext.requestDispatch();
                }
                return;
            }

            if (notifyReady) {
                notify();
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
