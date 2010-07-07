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

/**
 * A holder for queue elements loaded from the store.
 * 
 */
public interface RestoredElement<V> {
    /**
     * @return Gets the restored element (possibly null if not requested)
     * @throws Exception
     */
    public V getElement() throws Exception;

    /**
     * @return The element size.
     */
    int getElementSize();

    /**
     * @return A positive values indicating the expiration time if this
     *         element is expirable.
     */
    long getExpiration();

    /**
     * Returns the sequence number of this element in the queue
     * 
     * @return the sequence number of this element
     */
    long getSequenceNumber();

    /**
     * Gets the tracking number of the stored message.
     * 
     * @return the next sequence number
     */
    long getStoreTracking();

    /**
     * Gets the next sequence number in the queue after this one or -1 if
     * this is the last stored element
     * 
     * @return the next sequence number
     */
    long getNextSequenceNumber();
}