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
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.protobuf.AsciiBuffer;

/**
 * Base class for a {@link Dispatchable} {@link FlowControllable}
 * {@link IFlowQueue}.
 * 
 * @param <E>
 */
public abstract class AbstractFlowQueue<E> extends AbstractLimitedFlowSource<E> implements PersistentQueue<E>, FlowControllable<E>, IFlowQueue<E>, Dispatchable {

    protected IDispatcher dispatcher;
    protected DispatchContext dispatchContext;
    protected final Collection<IPollableFlowSource.FlowReadyListener<E>> readyListeners = new ArrayList<IPollableFlowSource.FlowReadyListener<E>>();
    private boolean notifyReady = false;
    protected boolean dispatching = false;
    protected int dispatchPriority = 0;
    protected QueueStoreHelper<E> storeHelper;
    protected FlowQueueListener listener = new FlowQueueListener()
    {
        public void onQueueException(IFlowQueue<?> queue, Throwable thrown) {
            System.out.println("Exception in queue: " + thrown.getMessage());
            thrown.printStackTrace();
        }
    };
    
    AsciiBuffer persistentQueueName;

    AbstractFlowQueue() {
        super();
    }

    protected AbstractFlowQueue(String name) {
        super(name);
    }

    public void setFlowQueueListener(FlowQueueListener listener) {
        this.listener = listener;
    }

    public final void add(E elem, ISourceController<?> source) {
        checkSave(elem, source);
        getSinkController(elem, source).add(elem, source);
    }

    public final boolean offer(E elem, ISourceController<?> source) {
        if (getSinkController(elem, source).offer(elem, source)) {
            checkSave(elem, source);
            return true;
        }
        return false;
    }

    private final void checkSave(E elem, ISourceController<?> source) {
        //TODO This is currently handled externally to the queue
        //but it would be nice to move it in here
        /*if (storeHelper != null && isElementPersistent(elem)) {
            try {
                storeHelper.save(elem, true);
            } catch (IOException e) {
                listener.onQueueException(this, e);
            }
        }*/
    }

    protected abstract ISinkController<E> getSinkController(E elem, ISourceController<?> source);

    public final boolean dispatch() {

        // while (pollingDispatch());
        // return true;

        return !pollingDispatch();
    }

    public final IFlowResource getFlowResource() {
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
        super.setFlowExecutor(dispatcher.createPriorityExecutor(dispatcher.getDispatchPriorities() - 1));
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

    /**
     * Enables persistence for this queue.
     */
    public void enablePersistence(QueueStoreHelper<E> storeHelper) {
        this.storeHelper = storeHelper;
    }

    /**
     * Called when an element is added from the queue's store.
     * 
     * @param elem
     *            The element
     * @param controller
     *            The store controller.
     */
    public void addFromStore(E elem, ISourceController<?> controller) {
        add(elem, controller);
    }

    /**
     * Subclasses should override this if they require persistence requires
     * saving to the store.
     * 
     * @param elem
     *            The element to check.
     */
    public boolean isElementPersistent(E elem) {
        return false;
    }

    public String toString() {
        return getResourceName();
    }

    /**
     * Returns the queue name used to indentify the queue in the store
     * 
     * @return
     */
    public AsciiBuffer getPeristentQueueName() {
        if (persistentQueueName == null) {
            String name = getResourceName();
            if (name != null) {
                persistentQueueName = new AsciiBuffer(name);
            }
        }
        return persistentQueueName;
    }

}
