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

public abstract class AbstractLimitedFlowSource<E> extends AbstractLimitedFlowResource<E> {

    protected boolean autoRelease = false;
    protected IFlowDrain<E> drain;

    protected AbstractLimitedFlowSource() {
        super();
    }

    protected AbstractLimitedFlowSource(String name) {
        super(name);
    }

    /**
     * Can be set to automatically release space on dequeue. When set the caller
     * does not need to call IFlowController.elementDispatched()
     * 
     * @param val
     */
    public synchronized void setAutoRelease(boolean val) {
        autoRelease = val;
    }

    /**
     * Returns whether or not this {@link IFlowSource} is set to automatically
     * release elements via {@link FlowController#elementDispatched(Object)
     * during dispatch. When auto release is set the caller <i>must</i> not call
     * {@link FlowController#elementDispatched(Object).
     */
    public synchronized boolean getAutoRelease() {
        return autoRelease;
    }

    public synchronized void setDrain(IFlowDrain<E> drain) {

        this.drain = drain;
    }

}
