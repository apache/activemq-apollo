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
package org.apache.activemq.apollo.broker;

import java.io.IOException;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.SaveableQueueElement;
import org.apache.activemq.util.buffer.AsciiBuffer;

public interface MessageDelivery {

    public Destination getDestination();

    /**
     * @return the message priority.
     */
    public int getPriority();

    public int getFlowLimiterSize();

    public AsciiBuffer getMsgId();

    public AsciiBuffer getProducerId();

    public <T> T asType(Class<T> type);

    /**
     * @return if the delivery is persistent
     */
    public boolean isPersistent();

    /**
     * @return a positive value indicates that the delivery has an expiration
     *         time.
     */
    public long getExpiration();

    /**
     * @return True if this message was read from the store.
     */
    public boolean isFromStore();

    /**
     * Returns true if this message requires acknowledgment.
     */
    public boolean isResponseRequired();

    /**
     * Called when the message's persistence requirements have been met. This
     * method must not restoreBlock.
     */
    public void onMessagePersisted();

    /**
     * @return A message record for the element that can be persisted to the
     *         message store.
     */
    public Store.MessageRecord createMessageRecord();

    /**
     * @return if the message is part of a transaction this returns the
     *         transaction id returned by {@link Transaction#getTid()} otherwise
     *         a value of -1 indicates that this delivery is not part of a
     *         transaction
     */
    public long getTransactionId();
    
    /**
     * Clears the transaction id. Called by the broker when a transacted message
     * is commited. 
     */
    public void clearTransactionId();

    public void beginDispatch(BrokerDatabase database); 
    
    public void finishDispatch(ISourceController<?> controller) throws IOException;
    
    /**
     * Sets the unique id used to identify this message in the store.
     * @param tracking The tracking to use.
     */
    public void setStoreTracking(long tracking);
    
    /**
     * Called by a queue to request that the element be persisted. The save is
     * done asynchronously, and depending on the state of the message delivery
     * may not even be issued to the underlying persistence store until a later
     * date. As such callers should use the acknowledge method to delete this
     * message rather than directly issuing a delete through the message store
     * itself. Direct delete from the message store is only safe once the
     * message has been saved to the store, so callers should request
     * notification of the save via the
     * {@link SaveableQueueElement#requestSaveNotify()} method before attempting
     * to acces the store directly.
     * 
     * @param sqe
     *            The element to save
     * @param controller
     *            A flow controller to use in the event that there isn't room in
     *            the database.
     * @param delayable
     *            Whether or not the save operation can be delayed.
     */
    public void persist(SaveableQueueElement<MessageDelivery> sqe, ISourceController<?> controller, boolean delayable);

    /**
     * Acknowledges the message for a particular queue. This will cause it to be
     * deleted from the message store.
     * 
     * @param sqe
     *            The queue element to delete
     */
    public void acknowledge(SaveableQueueElement<MessageDelivery> sqe);

    /**
     * Gets the tracking number used to identify this message in the message
     * store.
     * 
     * @return The store tracking or -1 if not set.
     */
    public long getStoreTracking();

    /**
     * Used to apply selectors against the message.
     * 
     * @return
     */
    public MessageEvaluationContext createMessageEvaluationContext();

}
