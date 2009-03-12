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

import java.util.HashMap;
import java.util.LinkedList;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

public class MultiFlowQueue<E> extends AbstractFlowQueue<E> {
    private final HashMap<Flow, SingleFlowQueue> flowQueues = new HashMap<Flow, SingleFlowQueue>();
    private final LinkedNodeList<SingleFlowQueue> readyQueues = new LinkedNodeList<SingleFlowQueue>();

    private final int perFlowWindow;
    private final int resumeThreshold;

    public MultiFlowQueue(String name, int perFlowWindow, int resumeThreshold) {
        super(name);
        this.perFlowWindow = perFlowWindow;
        this.resumeThreshold = resumeThreshold;
    }

    public final void flowElemAccepted(ISourceController<E> controller, E elem) {
        // We don't currently create a flow controller for this,
        // so this shouldn't be called.
        throw new UnsupportedOperationException();
    }

    public boolean offer(E elem, ISourceController<E> source) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public synchronized void add(E elem, ISourceController<E> source) {
        SingleFlowQueue queue = flowQueues.get(source.getFlow());
        if (queue == null) {
            queue = new SingleFlowQueue(source.getFlow(), new SizeLimiter<E>(perFlowWindow, resumeThreshold));
            flowQueues.put(source.getFlow(), queue);
            super.onFlowOpened(queue.controller);
        }
        queue.enqueue(elem, source);
    }

    public boolean pollingDispatch() {
        SingleFlowQueue queue = null;
        E elem = null;
        synchronized (this) {
            queue = peekReadyQueue();
            if (queue == null) {
                return false;
            }

            elem = queue.poll();
            if (elem == null) {

                unreadyQueue(queue);
                return false;
            }

            // rotate to have fair dispatch.
            queue.getList().rotate();
        }

        drain.drain(elem, queue.controller);
        return true;
    }

    public final E poll() {
        synchronized (this) {
            synchronized (this) {
                SingleFlowQueue queue = peekReadyQueue();
                if (queue == null) {
                    return null;
                }

                E elem = queue.poll();
                if (elem == null) {

                    unreadyQueue(queue);
                    return null;
                }

                // rotate to have fair dispatch.
                queue.getList().rotate();
                return elem;
            }
        }
    }

    public final boolean isDispatchReady() {
        return !readyQueues.isEmpty();
    }

    private SingleFlowQueue peekReadyQueue() {
        if (readyQueues.isEmpty()) {
            return null;
        }
        return readyQueues.getHead();
    }

    private void unreadyQueue(SingleFlowQueue node) {
        node.unlink();
    }

    private void addReadyQueue(SingleFlowQueue node) {
        readyQueues.addLast(node);
    }

    /**
     * Limits a flow that has potentially multiple sources.
     */
    private class SingleFlowQueue extends LinkedNode<SingleFlowQueue> implements FlowController.FlowControllable<E> {
        private final LinkedList<E> queue = new LinkedList<E>();
        final FlowController<E> controller;
        private boolean ready = false;

        SingleFlowQueue(Flow flow, IFlowLimiter<E> limiter) {
            this.controller = new FlowController<E>(this, flow, limiter, MultiFlowQueue.this);
        }

        final void enqueue(E elem, ISourceController<E> source) {
            controller.add(elem, source);
        }

        public IFlowSource<E> getFlowSource() {
            return MultiFlowQueue.this;
        }

        public IFlowSink<E> getFlowSink() {
            return MultiFlowQueue.this;
        }

        public void flowElemAccepted(ISourceController<E> controller, E elem) {

            synchronized (MultiFlowQueue.this) {
                queue.add(elem);
                if (!ready) {
                    addReadyQueue(this);
                    ready = true;
                }
                // Always request on new elements:
                notifyReady();
            }
        }

        private E poll() {
            E e = queue.poll();
            if (e == null) {
                ready = false;
            } else if (autoRelease) {
                controller.elementDispatched(e);
            }
            return e;
        }
    }

}
