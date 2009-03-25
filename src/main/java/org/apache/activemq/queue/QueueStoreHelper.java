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

public interface QueueStoreHelper<E> {

    /**
     * Requests that the helper start loading elements
     * saved for this queue. 
     * @param queue
     */
    public void startLoadingQueue();
    
    /**
     * Stop the helper from loading more elements stored for 
     * the queue
     * @param queue The queue.
     */
    public void stopLoadingQueue();
    
    /**
     * Checks to see if there are elements in the store 
     * for this queue. 
     * @param queue
     */
    public boolean hasStoredElements();
    
    /**
     * Deletes a given element for this queue from the store. 
     * @param elem The elem to delete. 
     */
    public void delete(E elem, boolean flush);
    
    /**
     * Saves an element to the store. 
     * @param elem The element to be saved. 
     */
    public void save(E elem, boolean flush);
    
}
