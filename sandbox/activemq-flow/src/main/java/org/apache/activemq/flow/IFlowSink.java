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

public interface IFlowSink<E> extends IFlowResource {
    /**
     * Adds an element to the sink. If limiter space in the sink is overflowed
     * by the element then it will restoreBlock the source controller.
     * 
     * @param elem
     *            The element to add to the sink.
     * @param source
     *            The source's flow controller.
     */
    public void add(E elem, ISourceController<?> source);

    /**
     * Offers an element to the sink. If there is no room available the source's
     * controller will be blocked and the element will not be added.
     * 
     * @param elem
     *            The element to offer
     * @param source
     *            The source's controller.
     * @return false if the element wasn't accepted.
     */
    public boolean offer(E elem, ISourceController<?> source);

    /**
     * Sets the executor to be used by the sink's {@link IFlowController}s.
     * Implementors must set the provided executor for all of the controllers
     * that it has open.
     * 
     * @param executor
     *            The executor.
     * @see ISinkController#setExecutor(Executor)
     */
    public void setFlowExecutor(Executor executor);
}
