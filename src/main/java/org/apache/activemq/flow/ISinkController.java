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

public interface ISinkController<E> extends IFlowSink<E> {
    /**
     * Defines required attributes for an entity that can be flow controlled.
     * 
     * @param <E>
     */
    public interface FlowControllable<E> {
        public void flowElemAccepted(ISourceController<E> controller, E elem);

        public IFlowSink<E> getFlowSink();

        public IFlowSource<E> getFlowSource();
    }

    /**
     * Used to get a notification when a blocked controller becomes unblocked
     * 
     * @param <E>
     */
    public interface FlowUnblockListener<E> {
        public void onFlowUnblocked(ISinkController<E> controller);
    }

    /**
     * Offers an element to the sink associated with this resource if space is
     * available. If no space is available false is returned. The element does
     * not get added to the overflow list.
     * 
     * @param elem
     *            The element to add.
     * @param controller
     *            the source flow controller.
     */
    public boolean offer(E elem, ISourceController<E> sourceController);

    /**
     * Adds an element to the sink associated with this resource if space is
     * available. If no space is available the source controller will be
     * blocked, and the source is responsible for tracking the space until this
     * controller resumes.
     * 
     * @param elem
     *            The element to add.
     * @param controller
     *            the source flow controller.
     */
    public void add(E elem, ISourceController<E> controller);

    /**
     * Called to check if this FlowController is currently being blocked
     * 
     * @return True if the flow is blocked.
     */
    public boolean isSinkBlocked();

    /**
     * Waits for a flow to become unblocked.
     * 
     * @param flow
     *            The flow.
     * @throws InterruptedException
     *             If interrupted while waiting.
     */
    public void waitForFlowUnblock() throws InterruptedException;

    /**
     * Sets a callback for the listener if this controller is currently blocked.
     * 
     * @param listener
     *            The listener.
     * @return True if a listener was registered false otherwise.
     */
    public boolean addUnblockListener(FlowUnblockListener<E> listener);

    public IFlowSink<E> getFlowSink();

}
