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
import org.apache.activemq.flow.ISourceController;

public interface QueueStore<K, V> {

    public interface PersistentQueue<K, V>
    {
        /**
         * Called to initialize the queue with values from the queue store. It is
         * illegal to start or add elements to an uninitialized queue, and doing so
         * will result in an {@link IllegalStateException}
         * 
         * @param sequenceMin
         *            The lowest sequence number in the store.
         * @param sequenceMax
         *            The max sequence number in the store.
         * @param count
         *            The number of messages in the queue
         * @param size
         *            The size of the messages in the queue
         */
        public void initialize(long sequenceMin, long sequenceMax, int count, long size);
        
        /**
         * Sets a store against which the queue can persist it's elements.
         * 
         * @param store
         *            The store.
         */
        public void setStore(QueueStore<K, V> store);

        /**
         * Sets a persistence policy for the queue which indicates how the queue
         * should persist its elements.
         * 
         * @param persistencePolicy
         *            The persistence policy for the queue.
         */
        public void setPersistencePolicy(PersistencePolicy<V> persistencePolicy);
        
        /**
         * Gets a descriptor for the queue. The descriptor is used to store the
         * queue in a {@link QueueStore}.
         * 
         * @return The queue descriptor.
         */
        public QueueDescriptor getDescriptor();

    }
    
    /**
     * Loads a series of elements for the specified queue. The loaded messages
     * are given to the provided {@link MessageRestoreListener}.
     * <p>
     * <b><i>NOTE:</i></b> This method uses the queue sequence number for the
     * message not the store tracking number.
     * 
     * @param queue
     *            The queue for which to load messages
     * @param recordOnly
     *            True if only the record data should be returned (excluding the
     *            element itself)
     * @param firstSequence
     *            The first queue sequence number to load (-1 starts at
     *            beginning)
     * @param maxSequence
     *            The maximum sequence number to load (-1 if no limit)
     * @param maxCount
     *            The maximum number of messages to load (-1 if no limit)
     * @param listener
     *            The listener to which restored elements should be passed.
     */
    public void restoreQueueElements(QueueDescriptor queue, boolean recordOnly, long firstSequence, long maxSequence, int maxCount, RestoreListener<V> listener);

    /**
     * Asynchronously deletes an element from the store.
     * 
     * @param element
     *            The element to delete.
     */
    public void deleteQueueElement(SaveableQueueElement<V> elem);

    /**
     * Asynchronously saves the given element to the store
     * 
     * @param elem
     *            The element to save
     * @param controller
     *            A flow controller to use in the event that there isn't room in
     *            the database.
     * @param delayable
     *            Whether or not the save operation can be delayed.
     * @throws Exception
     *             If there is an error saving the element.
     */
    public void persistQueueElement(SaveableQueueElement<V> elem, ISourceController<?> controller, boolean delayable);

    /**
     * Tests whether or not the given element came from the store. If so, a
     * queue must delete the element when it is finished with it
     * 
     * @param elem
     *            The element to check.
     * @return True if the element came from the store.
     */
    public boolean isFromStore(V elem);

    /**
     * Asynchronously adds a queue to the store.
     * 
     * @param queue
     *            The descriptor for the queue being added.
     */
    public void addQueue(QueueDescriptor queue);

    /**
     * Asynchronously deletes a queue and all of it's records from the store.
     * 
     * @param queue
     *            The descriptor for the queue to be deleted.
     */
    public void deleteQueue(QueueDescriptor queue);

}
