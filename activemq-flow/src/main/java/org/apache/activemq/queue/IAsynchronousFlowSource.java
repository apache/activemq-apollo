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

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.IFlowSource;

public interface IAsynchronousFlowSource<E> {

    /**
     * Sets an asynchronous dispatcher for this source. As elements become
     * available they will be dispatched to the worker pool.
     * 
     * @param workers
     *            The executor thread pool.
     * @param dispatcher
     *            The dispatcher to handle messages.
     * @param controller
     *            The controller for the flow to process in the worker pool.
     */
    public void setDispatcher(IDispatcher workers);

}
