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

import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowRelay;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISourceController;

/**
 * @author cmacnaug
 * 
 */
public abstract class AbstractFlowRelay<E> extends AbstractLimitedFlowResource<E> implements IFlowRelay<E> {

    protected boolean autoRelease = false;
    protected QueueDispatchTarget<E> drain;

    public AbstractFlowRelay() {
        super();
    }

    public AbstractFlowRelay(String name) {
        super(name);
    }

    /**
     * If set to true the source will automatically release limiter space
     * associated with {@link IFlowElem}s as they are dispacthed. If set to
     * false then the {@link IFlowDrain} must release space via a call to
     * {@link ISourceController#elementDispatched(IFlowElem)}.
     * 
     * @param autoRelease
     *            If the source should release limiter space for elements.
     */
    public void setAutoRelease(boolean autoRelease) {
        this.autoRelease = autoRelease;
    }

    /**
     * Returns whether or not this {@link IFlowSource} is set to automatically
     * release elements via {@link FlowController#elementDispatched(Object) during
     * dispatch. When auto release is set the caller <i>must</i> not call 
     * {@link FlowController#elementDispatched(Object). 
     * 
     * @return true if auto release is set, false otherwise. 
     */
    public boolean getAutoRelease() {
        return autoRelease;
    }

    /**
     * Sets the default drain for elements from this flow source. It will be
     * invoked to dispatch elements from the source.
     * 
     * @param drain
     *            The drain.
     */
    public void setDrain(QueueDispatchTarget<E> dispatchTarget) {
        this.drain = dispatchTarget;
    }

    public IFlowResource getFlowResource() {
        return this;
    }
}
