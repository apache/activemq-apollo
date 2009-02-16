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

import java.util.HashMap;
import java.util.HashSet;

public abstract class AbstractLimitedFlowResource<E> implements IFlowResource {
    private final HashSet<FlowLifeCycleListener> lifeCycleWatchers = new HashSet<FlowLifeCycleListener>();
    private final HashMap<Flow, IFlowController<E>> openControllers = new HashMap<Flow, IFlowController<E>>();

    private final long resourceId = RESOURCE_COUNTER.incrementAndGet();

    private String resourceName;

    protected AbstractLimitedFlowResource() {

    }

    protected AbstractLimitedFlowResource(String name) {
        this.resourceName = name;
    }

    public long getResourceId() {
        return resourceId;
    }

    public String getResourceName() {
        return resourceName;
    }

    protected void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public synchronized final void addFlowLifeCycleListener(FlowLifeCycleListener listener) {
        lifeCycleWatchers.add(listener);
        // Notify the watchers of all flows that are already open:
        for (IFlowController<E> controller : openControllers.values()) {
            listener.onFlowOpened(this, controller.getFlow());
        }
    }

    /**
     * Subclasses must call this whenever a new {@link ISinkController} is
     * opened.
     * 
     * @param controller
     *            The new controller.
     */
    protected synchronized final void onFlowOpened(IFlowController<E> controller) {
    	IFlowController<E> existing = openControllers.put(controller.getFlow(), controller);
        if (existing != null && existing != controller) {
            // Put the existing controller back:
            openControllers.put(controller.getFlow(), existing);
            throw new IllegalStateException("Flow already opened" + existing);
        }

        for (FlowLifeCycleListener listener : lifeCycleWatchers) {
            listener.onFlowOpened(this, controller.getFlow());
        }
    }

    protected synchronized final void onFlowClosed(Flow flow) {
    	IFlowController<E> existing = openControllers.remove(flow);

        if (existing != null) {
            for (FlowLifeCycleListener listener : lifeCycleWatchers) {
                listener.onFlowClosed(this, existing.getFlow());
            }
        }
    }

    public synchronized void removeFlowLifeCycleListener(FlowLifeCycleListener listener) {
        lifeCycleWatchers.remove(listener);
    }

    /**
     * Gets the flow controller corresponding to the specified flow.
     * 
     * @param flow
     *            The flow
     * @return The FlowController
     */
    public synchronized IFlowController<E> getFlowController(Flow flow) {
        return openControllers.get(flow);
    }
}
