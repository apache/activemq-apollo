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

/**
 * The control interface to a source. Sinks typically call back to a
 * ISourceController to suspend or resumed the dispatching of messages.
 * 
 * @param <E>
 */
public interface ISourceController<E> {

    /**
     * Gets the {@link IFlowResource} that this controller is controlling. 
     * @return The {@link IFlowResource} that this controller is controlling.
     */
    public IFlowResource getFlowResource();

    /**
     * Gets the flow that this controller is controlling.
     * 
     * @return
     */
    public Flow getFlow();

    /**
     * This is called when a particular flow is blocked for a resource
     * 
     * @param sinkController
     *            The sink controller blocking this source
     */
    public void onFlowBlock(ISinkController<?> sinkController);

    /**
     * Callback used with FlowControllers to get a notification that an
     * IFlowController has been resumed.
     * 
     * @param sinkController
     *            The sink controller that was unblocked.
     */
    public void onFlowResume(ISinkController<?> sinkController);

    /**
     * Must be called once the elements have been sent to downstream sinks.
     * 
     * @param elem
     *            The dispatched element.
     * @return
     */
    public void elementDispatched(E elem);

}
