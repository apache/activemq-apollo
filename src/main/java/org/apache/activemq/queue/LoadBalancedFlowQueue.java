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
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

/**
 */
public class LoadBalancedFlowQueue<E> extends AbstractFlowQueue<E> {
    private final LinkedList<E> queue = new LinkedList<E>();
    private final LinkedNodeList<SinkNode> readyConsumers = new LinkedNodeList<SinkNode>();
    private final HashMap<IFlowSink<E>, SinkNode> consumers = new HashMap<IFlowSink<E>, SinkNode>();

    private boolean strcitDispatch = true;

    private final FlowController<E> sinkController;

    private final ISourceController<E> sourceControler = new ISourceController<E>() {

        public Flow getFlow() {
            return sinkController.getFlow();
        }

        public IFlowSource<E> getFlowSource() {
            return LoadBalancedFlowQueue.this;
        }

        public void onFlowBlock(ISinkController<?> sink) {
            synchronized (LoadBalancedFlowQueue.this) {
                SinkNode node = consumers.get(sink);
                if (node != null) {
                    node.unlink();
                }
                // controller.onFlowBlock(sink);
            }

        }

        public void onFlowResume(ISinkController<?> sink) {
            synchronized (LoadBalancedFlowQueue.this) {
                SinkNode node = consumers.get(sink);
                if (node != null) {
                    // controller.onFlowResume(sink);
                    // Add to ready list if not there:
                    if (!node.isLinked()) {
                        boolean notify = false;
                        if (readyConsumers.isEmpty()) {
                            notify = true;
                        }

                        readyConsumers.addLast(node);
                        if (notify && !queue.isEmpty()) {
                            notifyReady();
                        }
                    }
                }
            }
        }

        public void elementDispatched(E elem) {
            // TODO Auto-generated method stub

        }

        public boolean isSourceBlocked() {
            // TODO Auto-generated method stub
            return false;
        }

    };

    /**
     * Creates a flow queue that can handle multiple flows.
     * 
     * @param flow
     *            The {@link Flow}
     * @param controller
     *            The FlowController if this queue is flow controlled:
     */
    public LoadBalancedFlowQueue(Flow flow, String name, long resourceId, IFlowLimiter<E> limiter) {
        super(name);
        this.sinkController = new FlowController<E>(getFlowControllableHook(), flow, limiter, this);
        super.onFlowOpened(sinkController);
    }

    protected final ISinkController<E> getSinkController(E elem, ISourceController<?> source) {
        return sinkController;
    }
    
    /**
     * Called when the controller accepts a message for this queue.
     */
    public synchronized void flowElemAccepted(ISourceController<E> controller, E elem) {
        queue.add(elem);
        if (!readyConsumers.isEmpty()) {
            notifyReady();
        }
    }

    public FlowController<E> getFlowController(Flow flow) {
        return sinkController;
    }

    public boolean isDispatchReady() {
        return !queue.isEmpty() && !readyConsumers.isEmpty();
    }

    public boolean pollingDispatch() {
        if (strcitDispatch) {
            return strictPollingDispatch();
        } else {
            return loosePollingDispatch();
        }
    }

    private boolean strictPollingDispatch() {

        SinkNode node = null;
        E elem = null;
        synchronized (this) {
            if (readyConsumers.isEmpty()) {
                return false;
            }
            // Get the next elem:
            elem = queue.peek();
            if (elem == null) {
                return false;
            }

            node = readyConsumers.getHead();
        }

        while (true) {

            boolean accepted = node.sink.offer(elem, sourceControler);

            synchronized (this) {
                if (accepted) {
                    queue.remove();
                    if (autoRelease) {
                        sinkController.elementDispatched(elem);
                    }
                    if (!readyConsumers.isEmpty()) {
                        readyConsumers.rotate();
                    }
                    return true;
                } else {
                    if (readyConsumers.isEmpty()) {
                        return false;
                    }
                    node = readyConsumers.getHead();
                }
            }
        }
    }

    private boolean loosePollingDispatch() {
        E elem = null;
        IFlowSink<E> sink = null;
        synchronized (this) {
            if (readyConsumers.isEmpty()) {
                return false;
            }

            // Get the next sink:
            sink = readyConsumers.getHead().sink;

            // Get the next elem:
            elem = poll();

            readyConsumers.rotate();
        }

        sink.add(elem, sourceControler);
        return true;
    }

    public final E poll() {
        synchronized (this) {
            E elem = queue.poll();
            // FIXME the release should really be done after dispatch.
            // doing it here saves us from having to resynchronize
            // after dispatch, but release limiter space too soon.
            if (elem != null) {
                if (autoRelease) {
                    sinkController.elementDispatched(elem);
                }
                return elem;
            }
            return null;
        }
    }

    public final void addSink(IFlowSink<E> sink) {
        synchronized (this) {
            SinkNode node = consumers.get(sink);
            if (node == null) {
                node = new SinkNode(sink);
                consumers.put(sink, node);
                readyConsumers.addLast(node);
                if (!queue.isEmpty()) {
                    notifyReady();
                }
            }
        }
    }

    private class SinkNode extends LinkedNode<SinkNode> {
        public final IFlowSink<E> sink;

        public SinkNode(IFlowSink<E> sink) {
            this.sink = sink;
        }

        @Override
        public String toString() {
            return sink.toString();
        }
    }

    public boolean isStrcitDispatch() {
        return strcitDispatch;
    }

    public void setStrcitDispatch(boolean strcitDispatch) {
        this.strcitDispatch = strcitDispatch;
    }
}
