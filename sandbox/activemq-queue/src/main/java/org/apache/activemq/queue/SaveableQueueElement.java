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

import org.apache.activemq.broker.store.QueueDescriptor;

public interface SaveableQueueElement<V> {

    /**
     * @return the descriptor of the queue for which the element should be
     *         saved.
     */
    public QueueDescriptor getQueueDescriptor();

    /**
     * @return the element to save.
     */
    public V getElement();

    /**
     * @return the sequence number of the element in the queue
     * 
     * 
     */
    public long getSequenceNumber();

    /**
     * @return a return value of true will cause {@link #notifySave()} to
     *         called when this element is persisted
     */
    public boolean requestSaveNotify();
    
    /**
     * @return The size of the element in memory
     */
    public int getLimiterSize();

    /**
     * Called when the element has been saved.
     */
    public void notifySave();
}