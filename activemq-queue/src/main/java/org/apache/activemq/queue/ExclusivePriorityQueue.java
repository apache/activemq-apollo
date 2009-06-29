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

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.PriorityFlowController;
import org.apache.activemq.flow.PrioritySizeLimiter;
import org.apache.activemq.util.PriorityLinkedList;
import org.apache.activemq.util.list.LinkedNode;

/**
 */
public class ExclusivePriorityQueue<E> extends AbstractFlowQueue<E> implements IFlowQueue<E> {

    private final PriorityLinkedList<PriorityNode> queue;

    private class PriorityNode extends LinkedNode<PriorityNode> {
        E elem;
        int prio;
    }

    private final PriorityFlowController<E> controller;
    private final PrioritySizeLimiter<E> limiter;

    /**
     * Creates a flow queue that can handle multiple flows.
     * 
     * @param priority
     * @param flow
     *            The {@link Flow}
     * @param capacity
     * @param resume
     * @param controller
     *            The FlowController if this queue is flow controlled:
     */
    public ExclusivePriorityQueue(Flow flow, String name, PrioritySizeLimiter<E> limiter) {
        super(name);
        this.limiter = limiter;
        this.queue = new PriorityLinkedList<PriorityNode>(10);
        this.controller = new PriorityFlowController<E>(getFlowControllableHook(), flow, limiter, this);

    }

    
    public void add(E elem, ISourceController<?> source) {
        controller.add(elem, source);
    }

    public boolean offer(E elem, ISourceController<?> source) {
        return controller.offer(elem, source);
    }
    
    /**
     * Called when the controller accepts a message for this queue.
     */
    public synchronized void flowElemAccepted(ISourceController<E> controller, E elem) {
        PriorityNode node = new PriorityNode();
        node.elem = elem;
        node.prio = limiter.getPriorityMapper().map(elem);

        queue.add(node, node.prio);
        notifyReady();
    }

    public FlowController<E> getFlowController(Flow flow) {
        // TODO:
        return null;
    }

    public boolean isDispatchReady() {
        return !queue.isEmpty();
    }

    public boolean pollingDispatch() {
        E elem = poll();
        if (elem != null) {
            if (sub != null) {
                sub.add(elem, controller, null);
            } else {
                //TODO remove drain:
                drain.drain(elem, controller);
            }
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

    @Override
    public String toString() {
        return getResourceName();
    }
}
