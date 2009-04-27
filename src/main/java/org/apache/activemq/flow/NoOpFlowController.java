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

import java.util.concurrent.Executor;

public class NoOpFlowController<E> implements ISinkController<E> {
    private final IFlowSource<E> source;
    private final Flow flow;

    public NoOpFlowController(IFlowSource<E> source, Flow flow) {
        this.source = source;
        this.flow = flow;
    }

    public IFlowSource<E> getFlowSource() {
        return source;
    }

    public Flow getFlow() {
        // TODO Auto-generated method stub
        return flow;
    }

    public boolean isSinkBlocked() {
        return false;
    }

    public void onFlowBlock(IFlowSink<E> sink) {
        // Noop
    }

    public void onFlowResume(IFlowSink<E> sink) {
        // Noop
    }

    /**
     * Must be called once the elements have been sent to downstream sinks.
     * 
     * @param elem
     *            The dispatched element.
     * @return
     */
    public void elementDispatched(E elem) {
        // No op for basic flow controller
    }

    public String toString() {
        return "DISABLED Flow Controller for: " + source;
    }

    public boolean offer(E elem, ISourceController<?> sourceController) {
        throw new UnsupportedOperationException();
    }

    public void add(E elem, ISourceController<?> controller) {
        throw new UnsupportedOperationException();
    }

    public void waitForFlowUnblock() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    /**
     * Sets a callback for the listener if this controller is currently blocked.
     * 
     * @param listener
     *            The listener.
     * @return True if a listener was registered false otherwise.
     */
    public boolean addUnblockListener(FlowUnblockListener<E> listener) {
        return false;
    }

    public IFlowResource getFlowResource() {
        return source;
    }

    public void setExecutor(Executor executor) {
        // Don't need an executor since we don't restoreBlock.
    }
}
