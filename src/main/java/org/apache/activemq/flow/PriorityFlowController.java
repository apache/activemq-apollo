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

import org.apache.activemq.queue.Mapper;

public class PriorityFlowController<E> implements ISourceController<E>, ISinkController<E> {

    private final Object mutex;
    private final FlowController<E> controllers[];
    private final PrioritySizeLimiter<E> limiter;

    private Mapper<Integer, E> priorityMapper;

    private final Flow flow;
    private final FlowControllable<E> controllable;

    public PriorityFlowController(int priorities, FlowControllable<E> controllable, Flow flow, Object mutex, int capacity, int resume) {
        this.controllable = controllable;
        this.flow = flow;
        this.mutex = mutex;
        this.limiter = new PrioritySizeLimiter<E>(capacity, resume, priorities);
        this.limiter.setPriorityMapper(priorityMapper);
        this.controllers = createControlerArray(priorities);
        for (int i = 0; i < priorities; i++) {
            this.controllers[i] = new FlowController<E>(controllable, flow, limiter.getPriorityLimter(i), mutex);
        }
    }

    @SuppressWarnings("unchecked")
    private FlowController<E>[] createControlerArray(int priorities) {
        return new FlowController[priorities];
    }

    // /////////////////////////////////////////////////////////////////
    // ISinkController interface impl.
    // /////////////////////////////////////////////////////////////////

    public boolean offer(E elem, ISourceController<E> controller) {
        int prio = priorityMapper.map(elem);
        return controllers[prio].offer(elem, controller);
    }

    public void add(E elem, ISourceController<E> controller) {
        int prio = priorityMapper.map(elem);
        controllers[prio].add(elem, controller);
    }

    public boolean isSinkBlocked() {
        synchronized (mutex) {
            return limiter.getThrottled();
        }
    }

    public boolean addUnblockListener(org.apache.activemq.flow.ISinkController.FlowUnblockListener<E> listener) {
        boolean rc = false;
        for (int i = 0; i < controllers.length; i++) {
            rc |= this.controllers[i].addUnblockListener(listener);
        }
        return rc;
    }

    public void waitForFlowUnblock() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    // /////////////////////////////////////////////////////////////////
    // ISourceController interface impl.
    // /////////////////////////////////////////////////////////////////

    public void elementDispatched(E elem) {
        FlowController<E> controler = controllers[priorityMapper.map(elem)];
        controler.elementDispatched(elem);
    }

    public Flow getFlow() {
        return flow;
    }

    public IFlowSource<E> getFlowSource() {
        return controllable.getFlowSource();
    }

    public void onFlowBlock(ISinkController<E> sink) {
        for (int i = 0; i < controllers.length; i++) {
            controllers[i].onFlowBlock(sink);
        }
    }

    public void onFlowResume(ISinkController<E> sink) {
        for (int i = 0; i < controllers.length; i++) {
            controllers[i].onFlowBlock(sink);
        }
    }

    public boolean isSourceBlocked() {
        return false;
    }

    // /////////////////////////////////////////////////////////////////
    // Getters and Setters
    // /////////////////////////////////////////////////////////////////

    public Mapper<Integer, E> getPriorityMapper() {
        return priorityMapper;
    }

    public void setPriorityMapper(Mapper<Integer, E> priorityMapper) {
        this.priorityMapper = priorityMapper;
        limiter.setPriorityMapper(priorityMapper);
    }

    public IFlowSink<E> getFlowSink() {
        return controllable.getFlowSink();
    }
}
