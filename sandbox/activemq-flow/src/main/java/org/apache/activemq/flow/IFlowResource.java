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

import java.util.concurrent.atomic.AtomicLong;

public interface IFlowResource {

    /**
     * A listener for when new flows are opened and closed for this resource.
     */
    public interface FlowLifeCycleListener {
        public void onFlowOpened(IFlowResource source, Flow flow);

        public void onFlowClosed(IFlowResource source, Flow flow);
    }

    /**
     * Adds a {@link FlowLifeCycleListener} for flows opened by the source.
     * 
     * @param listener
     *            The listener.
     */
    public void addFlowLifeCycleListener(FlowLifeCycleListener listener);

    /**
     * Called to remove a previously added {@link FlowLifeCycleListener}.
     * 
     * @param listener
     *            The listener.
     */
    public void removeFlowLifeCycleListener(FlowLifeCycleListener listener);

    static final public AtomicLong RESOURCE_COUNTER = new AtomicLong();

    /**
     * Gets the unique resource id associated with this resource
     * 
     * @retrun the unique resource id.
     */
    public long getResourceId();

    public String getResourceName();
}
