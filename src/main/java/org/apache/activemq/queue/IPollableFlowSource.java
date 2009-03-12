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

import org.apache.activemq.flow.IFlowSource;

public interface IPollableFlowSource<E> extends IFlowSource<E> {

    /**
     * Callback used to indicate that a PollableFlowSource is ready for
     * dispatch.
     * 
     * @param <E>
     */
    public interface FlowReadyListener<E> {
        public void onFlowReady(IPollableFlowSource<E> source);
    }

    /**
     * Sets a listener to indicate when there are elements available for
     * dispatch from this source.
     * 
     * @param listener
     *            The listener.
     */
    public void addFlowReadyListener(FlowReadyListener<E> listener);

    /**
     * Dispatches the next available element returning false if there were no
     * elements available for dispatch.
     * 
     * @return False if there were no elements to dispatch.
     */
    public boolean pollingDispatch();

    /**
     * @return True if there are elements ready to dispatch.
     */
    public boolean isDispatchReady();

    /**
     * Polls for the next element
     * 
     * @return The next element or null if none are ready
     */
    public E poll();

}
