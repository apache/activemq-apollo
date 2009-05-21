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

import org.apache.activemq.flow.ISourceController;

/**
 * @author cmacnaug
 *
 */
public interface QueueDispatchTarget<E> {
    
    /**
     * Used by a FlowSource that is being dispatched to drain it's elements.
     * The implementor is responsible for calling {@link ISourceController#elementDispatched(Object)
     * when the element has been dispatched to all downstream sinks unless the 
     * IFlowSource#getAutoRelease() is set to true.
     * 
     * @param elem The element being drained
     * @param controller The source's controller
     */
    public void drain(E elem, ISourceController<E> controller);
}
