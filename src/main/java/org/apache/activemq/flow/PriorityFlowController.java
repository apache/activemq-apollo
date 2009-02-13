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

import java.util.ArrayList;

public class PriorityFlowController<E> implements ISourceController<E>, ISinkController<E> {

    private final Object mutex;
    private final ArrayList<FlowController<E>> controllers;
    private final PrioritySizeLimiter<E> limiter;

    private final Flow flow;
    private final FlowControllable<E> controllable;

    public PriorityFlowController(FlowControllable<E> controllable, Flow flow, PrioritySizeLimiter<E> limiter, Object mutex) {
        this.controllable = controllable;
        this.flow = flow;
        this.mutex = mutex;
        this.limiter =  limiter;
        this.controllers = new ArrayList<FlowController<E>>(limiter.getPriorities());
        for (int i = 0; i < limiter.getPriorities(); i++) {
            controllers.add(new FlowController<E>(controllable, flow, limiter.getPriorityLimter(i), mutex));
        }
    }

    // /////////////////////////////////////////////////////////////////
    // ISinkController interface impl.
    // /////////////////////////////////////////////////////////////////

    public boolean offer(E elem, ISourceController<E> controller) {
        int prio = limiter.getPriorityMapper().map(elem);
        return controllers.get(prio).offer(elem, controller);
    }

    public void add(E elem, ISourceController<E> controller) {
        int prio = limiter.getPriorityMapper().map(elem);
        controllers.get(prio).add(elem, controller);
    }

    public boolean isSinkBlocked() {
        synchronized (mutex) {
            return limiter.getThrottled();
        }
    }

    public boolean addUnblockListener(org.apache.activemq.flow.ISinkController.FlowUnblockListener<E> listener) {
        boolean rc = false;
        for (int i = 0; i < controllers.size(); i++) {
            rc |= this.controllers.get(i).addUnblockListener(listener);
        }
        return rc;
    }

    public void waitForFlowUnblock() throws InterruptedException {
        throw new UnsupportedOperationException();
    }
    
    public long getResourceId() {
        IFlowSink<E> flowSink = getFlowSink();
        if( flowSink!=null ) {
            return flowSink.getResourceId();
        }
        return 0;
    }

    public String getResourceName() {
        IFlowSink<E> flowSink = getFlowSink();
        if( flowSink!=null ) {
            return flowSink.getResourceName();
        }
        return null;
    }

    public void addFlowLifeCycleListener(FlowLifeCycleListener listener) {
        IFlowSink<E> flowSink = getFlowSink();
        if( flowSink!=null ) {
            flowSink.addFlowLifeCycleListener(listener);
        }
    }
    
    public void removeFlowLifeCycleListener(FlowLifeCycleListener listener) {
        IFlowSink<E> flowSink = getFlowSink();
        if( flowSink!=null ) {
            flowSink.removeFlowLifeCycleListener(listener);
        }
    }


    // /////////////////////////////////////////////////////////////////
    // ISourceController interface impl.
    // /////////////////////////////////////////////////////////////////

    public void elementDispatched(E elem) {
        Integer prio = limiter.getPriorityMapper().map(elem);
        FlowController<E> controler = controllers.get(prio);
        controler.elementDispatched(elem);
    }

    public Flow getFlow() {
        return flow;
    }

    public IFlowSource<E> getFlowSource() {
        return controllable.getFlowSource();
    }

    public void onFlowBlock(ISinkController<E> sink) {
        for (int i = 0; i < controllers.size(); i++) {
            controllers.get(i).onFlowBlock(sink);
        }
    }

    public void onFlowResume(ISinkController<E> sink) {
        for (int i = 0; i < controllers.size(); i++) {
            controllers.get(i).onFlowBlock(sink);
        }
    }

    public boolean isSourceBlocked() {
        return false;
    }

    // /////////////////////////////////////////////////////////////////
    // Getters and Setters
    // /////////////////////////////////////////////////////////////////

    public IFlowSink<E> getFlowSink() {
        return controllable.getFlowSink();
    }
}
