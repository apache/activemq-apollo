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
import org.apache.activemq.protobuf.AsciiBuffer;

public interface PersistentQueue<E> {


    /**
     * Called when an element is added from the queue's store.
     * 
     * @param elem
     *            The element
     * @param controller
     *            The store controller.
     */
    public void addFromStore(E elem, ISourceController<?> controller);

    /**
     * Implementors implement this to indicate whether or not the given element
     * requires saving to the store.
     * 
     * @param elem
     *            The element to check.
     */
    public boolean isElementPersistent(E elem);
    
    /**
     * Returns the queue name used to indentify the queue in the store
     * @return
     */
    public AsciiBuffer getPeristentQueueName();
    
}
