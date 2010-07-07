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

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSizeLimiter;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.CursoredQueue.Cursor;
import org.apache.activemq.queue.CursoredQueue.QueueElement;
import org.apache.activemq.queue.Subscription.SubscriptionDelivery;
import org.apache.activemq.util.Mapper;
import org.fusesource.hawtbuf.AsciiBuffer;

public class ExclusivePersistentQueue<K, E> extends AbstractFlowQueue<E> implements IQueue<K, E> {
    private CursoredQueue<E> queue;
    private final FlowController<E> controller;
    private final IFlowSizeLimiter<E> limiter;
    private Cursor<E> cursor;
    private final QueueDescriptor queueDescriptor;
    private PersistencePolicy<E> persistencePolicy;
    private QueueStore<K, E> queueStore;
    private Mapper<Long, E> expirationMapper;
    private boolean initialized;
    private Subscription<E> subscription;
    private ISourceController<E> sourceController;
    protected boolean subBlocked = false;

    /**
     * 
     * 
     * @param flow
     *            The {@link Flow}
     * @param name
     *            The name of the queue.
     * @param limiter
     *            The size limiter for the queue.
     */
    public ExclusivePersistentQueue(String name, IFlowSizeLimiter<E> limiter) {
        super(name);
        this.queueDescriptor = new QueueDescriptor();
        this.limiter = limiter;
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.EXCLUSIVE);

        //TODO flow should be serialized as part of the subscription. 
        this.controller = new FlowController<E>(null, new Flow(name, false), limiter, this);
        this.controller.useOverFlowQueue(false);
        super.onFlowOpened(controller);

        sourceController = new ISourceController<E>() {

            public void elementDispatched(E elem) {
                // No Op
            }

            public Flow getFlow() {
                return controller.getFlow();
            }

            public IFlowResource getFlowResource() {
                return ExclusivePersistentQueue.this;
            }

            public void onFlowBlock(ISinkController<?> sinkController) {
                synchronized (ExclusivePersistentQueue.this) {
                    subBlocked = true;
                }
            }

            public void onFlowResume(ISinkController<?> sinkController) {
                synchronized (ExclusivePersistentQueue.this) {
                    subBlocked = false;
                    if (isDispatchReady()) {
                        notifyReady();
                    }
                }
            }
        };
    }

    public synchronized void remove(long key) {
        queue.remove(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.queue.QueueStore.PersistentQueue#initialize(long,
     * long, int, long)
     */
    public synchronized void initialize(long sequenceMin, long sequenceMax, int count, long size) {
        if (initialized) {
            throw new IllegalStateException("Queue already initialized");
        }

        //Initialize the limiter:
        if (count > 0) {
            limiter.add(count, size);
        }

        queue = new CursoredQueue<E>(persistencePolicy, expirationMapper, controller.getFlow(), queueDescriptor, queueStore, this) {

            @Override
            protected Object getMutex() {
                return ExclusivePersistentQueue.this;
            }

            @Override
            protected void onElementRemoved(QueueElement<E> qe) {
                synchronized (ExclusivePersistentQueue.this) {
                    limiter.remove(1, qe.getLimiterSize());
                }
            }

            @Override
            protected void onElementReenqueued(QueueElement<E> qe, ISourceController<?> source) {
                synchronized (ExclusivePersistentQueue.this) {
                    if (isDispatchReady()) {
                        notifyReady();
                    }
                }
            }

            @Override
            protected int getElementSize(E elem) {
                return limiter.getElementSize(elem);
            }

            @Override
            protected void requestDispatch() {
                notifyReady();
            }
        };

        queue.initialize(sequenceMin, sequenceMax, count, size);

        //Open a cursor for the queue:
        FlowController<QueueElement<E>> memoryController = null;
        if (persistencePolicy.isPagingEnabled()) {
            IFlowSizeLimiter<QueueElement<E>> limiter = new SizeLimiter<QueueElement<E>>(persistencePolicy.getPagingInMemorySize(), persistencePolicy.getPagingInMemorySize() / 2) {
                @Override
                public int getElementSize(QueueElement<E> qe) {
                    return qe.getLimiterSize();
                };
            };

            memoryController = new FlowController<QueueElement<E>>(null, controller.getFlow(), limiter, this) {
                @Override
                public IFlowResource getFlowResource() {
                    return ExclusivePersistentQueue.this;
                }
            };
            controller.useOverFlowQueue(false);
            controller.setExecutor(dispatcher.getGlobalQueue(DispatchPriority.HIGH));
        }

        cursor = queue.openCursor(getResourceName(), memoryController, true, true);
        cursor.reset(sequenceMin);
        cursor.activate();

        initialized = true;
    }

    public synchronized void addSubscription(Subscription<E> sub) {
        if (subscription != null) {
            if (subscription != sub) {
                //TODO change this to something other than a runtime exception:
                throw new IllegalStateException();
            }
            return;
        }
        this.subscription = sub;
        subBlocked = false;
        if (isDispatchReady()) {
            notifyReady();
        }
    }

    public synchronized boolean removeSubscription(Subscription<E> sub) {
        if (sub == subscription) {
            subscription = null;
            cursor.reset(queue.getFirstSequence());
            return true;
        } else {
            return false;
        }
    }

    protected final ISinkController<E> getSinkController(E elem, ISourceController<?> source) {
        return controller;
    }

    public void add(E elem, ISourceController<?> source) {
        synchronized (this) {
            assert initialized;
            controller.add(elem, source);
            accepted(source, elem);
        }
    }

    public boolean offer(E elem, ISourceController<?> source) {
        synchronized (this) {
            assert initialized;
            if (controller.offer(elem, source)) {
                accepted(source, elem);
                return true;
            }
            return false;
        }
    }

    /**
     * Called when the controller accepts a message for this queue.
     */
    public synchronized void flowElemAccepted(ISourceController<E> controller, E elem) {
        accepted(null, elem);
    }

    private final void accepted(ISourceController<?> source, E elem) {
        queue.add(source, elem);
        if (isDispatchReady()) {
            notifyReady();
        }
    }

    public synchronized void start() {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        if (!started) {
            super.start();
            queue.start();
        }
    }

    public synchronized void stop() {
        if (started) {
            super.stop();
            queue.stop();
        }
    }

    public void shutdown(final Runnable onShutdown) {
        super.shutdown(new Runnable() {
            public void run() {
                synchronized (ExclusivePersistentQueue.this) {
                    queue.shutdown();
                }
                if( onShutdown!=null ) {
                    onShutdown.run();
                }
            }
        });
    }

    public FlowController<E> getFlowController(Flow flow) {
        return controller;
    }

    public final boolean isDispatchReady() {
        if (started && subscription != null && !subBlocked && cursor.isReady()) {
            return true;
        }

        if (queue.needsDispatch()) {
            return true;
        }

        return false;
    }

    public synchronized final boolean pollingDispatch() {
        queue.dispatch();
        if (started && subscription != null && !subBlocked) {
            QueueElement<E> qe = cursor.getNext();
            if (qe != null) {
                // If the sub doesn't remove on dispatch set an ack listener:
                SubscriptionDelivery<E> callback = subscription.isRemoveOnDispatch(qe.elem) ? null : qe;

                // See if the sink has room:
                qe.setAcquired(subscription);
                if (subscription.offer(qe.elem, sourceController, callback)) {
                    // If remove on dispatch acknowledge now:
                    if (callback == null) {
                        qe.acknowledge();
                    }
                } else {
                    qe.setAcquired(null);
                }
            }
        }

        return isDispatchReady();
    }

    public E poll() {
        throw new UnsupportedOperationException("poll not supported for exclusive queue");
        //        
        //        synchronized (this) {
        //            if (!started) {
        //                return null;
        //            }
        //
        //            QueueElement<E> qe = cursor.getNext();
        //
        //            // FIXME the release should really be done after dispatch.
        //            // doing it here saves us from having to resynchronize
        //            // after dispatch, but release limiter space too soon.
        //            if (qe != null) {
        //                if (autoRelease) {
        //                    controller.elementDispatched(qe.getElement());
        //                }
        //                return qe.getElement();
        //            }
        //            return null;
        //        }
    }

    @Override
    public String toString() {
        return "SingleFlowQueue:" + getResourceName();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.queue.QueueStore.PersistentQueue#getDescriptor()
     */
    public QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.queue.QueueStore.PersistentQueue#setPersistencePolicy
     * (org.apache.activemq.queue.PersistencePolicy)
     */
    public void setPersistencePolicy(PersistencePolicy<E> persistencePolicy) {
        this.persistencePolicy = persistencePolicy;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.queue.QueueStore.PersistentQueue#setStore(org.apache
     * .activemq.queue.QueueStore)
     */
    public void setStore(QueueStore<K, E> store) {
        this.queueStore = store;
    }

    /**
     * @param expirationMapper
     */
    public void setExpirationMapper(Mapper<Long, E> expirationMapper) {
        this.expirationMapper = expirationMapper;

    }

    /**
     * @return The size of the elements in this queue or -1 if not yet known.
     */
    public synchronized long getEnqueuedSize() {
        if (!initialized) {
            return -1;
        }
        return limiter.getSize();
    }

    /**
     * @return The count of the elements in this queue or -1 if not yet known.
     */
    public synchronized int getEnqueuedCount() {
        if (!initialized) {
            return -1;
        }
        return queue.getEnqueuedCount();
    }
}
