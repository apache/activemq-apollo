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

import org.apache.activemq.dispatch.PriorityLinkedList;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.ISourceController;
import org.apache.kahadb.util.LinkedNode;

/**
 */
public class SingleFlowPriorityQueue<E> extends AbstractFlowQueue<E> {

    private final PriorityLinkedList<PriorityNode> queue;
    private Mapper<Integer, E> priorityMapper;
    private final FlowController<E> controller;

    private class PriorityNode extends LinkedNode<PriorityNode> {
        E elem;
        int prio;
    }

    private int messagePriority = 0;

    /**
     * Creates a flow queue that can handle multiple flows.
     * 
     * @param flow
     *            The {@link Flow}
     * @param controller
     *            The FlowController if this queue is flow controlled:
     */
    public SingleFlowPriorityQueue(Flow flow, String name, IFlowLimiter<E> limiter) {
        super(name);
        this.queue = new PriorityLinkedList<PriorityNode>(10);
        this.controller = new FlowController<E>(getFlowControllableHook(), flow, limiter, this);
        super.onFlowOpened(controller);
    }

    public boolean offer(E elem, ISourceController<?> source) {
        return controller.offer(elem, source);
    }

    /**
     * Performs a limited add to the queue.
     */
    public final void add(E elem, ISourceController<?> source) {
        controller.add(elem, source);
    }

    /**
     * Called when the controller accepts a message for this queue.
     */
    public final void flowElemAccepted(ISourceController<E> controller, E elem) {
        PriorityNode node = new PriorityNode();
        node.elem = elem;
        node.prio = priorityMapper.map(elem);

        synchronized (this) {
            queue.add(node, node.prio);
            updatePriority();
            notifyReady();
        }
    }

    public FlowController<E> getFlowController(Flow flow) {
        return controller;
    }

    public final boolean isDispatchReady() {
        return !queue.isEmpty();
    }

    public boolean pollingDispatch() {
        E elem = poll();
        if (elem != null) {
            drain.drain(elem, controller);
            return true;
        } else {
            return false;
        }
    }

    public final E poll() {
        synchronized (this) {
            PriorityNode node = queue.poll();
            // FIXME the release should really be done after dispatch.
            // doing it here saves us from having to resynchronize
            // after dispatch, but release limiter space too soon.
            if (node != null) {
                if (autoRelease) {
                    controller.elementDispatched(node.elem);
                }
                return node.elem;
            }
            return null;
        }
    }

    private final void updatePriority() {
        if (dispatchContext != null) {
            int newPrio = Math.max(queue.getHighestPriority(), dispatchPriority);
            if (messagePriority != newPrio) {
                messagePriority = newPrio;
                dispatchContext.updatePriority(messagePriority);
            }

        }
    }

    public Mapper<Integer, E> getPriorityMapper() {
        return priorityMapper;
    }

    public void setPriorityMapper(Mapper<Integer, E> priorityMapper) {
        this.priorityMapper = priorityMapper;
    }
}
