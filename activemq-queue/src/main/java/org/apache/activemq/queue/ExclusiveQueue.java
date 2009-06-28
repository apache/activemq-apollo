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

import java.util.LinkedList;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.ISourceController;

public class ExclusiveQueue<E> extends AbstractFlowQueue<E> {
    private final LinkedList<E> queue = new LinkedList<E>();
    private final FlowController<E> controller;

    /**
     * Creates a flow queue that can handle multiple flows.
     * 
     * @param flow
     *            The {@link Flow}
     * @param controller
     *            The FlowController if this queue is flow controlled:
     */
    public ExclusiveQueue(Flow flow, String name, IFlowLimiter<E> limiter) {
        super(name);
        this.controller = new FlowController<E>(getFlowControllableHook(), flow, limiter, this);
        super.onFlowOpened(controller);
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
        queue.add(elem);
        if (started) {
            notifyReady();
        }
    }

    public FlowController<E> getFlowController(Flow flow) {
        return controller;
    }

    public final boolean isDispatchReady() {
        return !queue.isEmpty() && started;
    }

    public final boolean pollingDispatch() {
        E elem = poll();

        if (elem != null) {
            if (sub != null) {
                sub.add(elem, controller, null);
            } else {
                drain.drain(elem, controller);
            }

            return true;
        } else {
            return false;
        }
    }

    public final E poll() {
        synchronized (this) {
            if (!started) {
                return null;
            }

            E elem = queue.poll();
            // FIXME the release should really be done after dispatch.
            // doing it here saves us from having to resynchronize
            // after dispatch, but release limiter space too soon.
            if (elem != null) {
                if (autoRelease) {
                    controller.elementDispatched(elem);
                }
                return elem;
            }
            return null;
        }
    }

    @Override
    public String toString() {
        return "SingleFlowQueue:" + getResourceName();
    }
}
