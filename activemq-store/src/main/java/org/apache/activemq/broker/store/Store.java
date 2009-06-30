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
package org.apache.activemq.broker.store;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;

import org.apache.activemq.Service;
import org.apache.activemq.queue.QueueDescriptor;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Interface to persistently store and access data needed by the messaging
 * system.
 */
public interface Store extends Service {

    public class FatalStoreException extends RuntimeException {
        private static final long serialVersionUID = 1122460895970375737L;

        public FatalStoreException() {
        }

        public FatalStoreException(String message, Throwable cause) {
            super(message, cause);
        }

        public FatalStoreException(String message) {
            super(message);
        }

        public FatalStoreException(Throwable cause) {
            super(cause);
        }
    }

    public class DuplicateKeyException extends Exception {
        private static final long serialVersionUID = -477567614452245482L;

        public DuplicateKeyException() {
        }

        public DuplicateKeyException(String message) {
            super(message);
        }

        public DuplicateKeyException(String message, Throwable cause) {
            super(message, cause);
        }

        public DuplicateKeyException(Throwable cause) {
            super(cause);
        }
    }

    public class KeyNotFoundException extends Exception {
        private static final long serialVersionUID = -2570252319033659546L;

        public KeyNotFoundException() {
            super();
        }

        public KeyNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }

        public KeyNotFoundException(String message) {
            super(message);
        }

        public KeyNotFoundException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Sets the store's root directory;
     * 
     * @param directory
     *            The root directory
     * 
     */
    public void setStoreDirectory(File directory);

    /**
     * Gets the store's root directory;
     */
    public File getStoreDirectory();

    /**
     * Indicates that all messages should be deleted on startup
     * 
     * @param val
     *            True if all messages should be deleted on startup
     */
    public void setDeleteAllMessages(boolean val);

    /**
     * @return a unique sequential store tracking number.
     */
    public long allocateStoreTracking();

    /**
     * @return A new store Session.
     */
    public Session getSession();

    /**
     * This interface is used to execute transacted code.
     * 
     * It is used by the {@link Store#execute(Callback)} method, often as
     * anonymous class.
     */
    public interface Callback<R, T extends Exception> {

        /**
         * Gets called by the {@link Store#execute(Callback)} method within a
         * transactional context. If any exception is thrown including Runtime
         * exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @return the result of the Callback
         * @throws T
         *             if an system error occured while executing the
         *             operations.
         */
        public R execute(Session session) throws T;
    }

    /**
     * Convenience class which allows you to implement {@link Callback} classes
     * which do not return a value.
     */
    public abstract class VoidCallback<T extends Exception> implements Callback<Object, T> {

        /**
         * Gets called by the {@link Store#execute(VoidCallback)} method within
         * a transactional context. If any exception is thrown including Runtime
         * exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @throws T
         *             if an error occurs and the transaction should get rolled
         *             back
         */
        abstract public void run(Session session) throws T;

        final public Object execute(Session session) throws T {
            run(session);
            return null;
        }
    }

    public static class SubscriptionRecord {

        AsciiBuffer name;
        AsciiBuffer selector;
        AsciiBuffer destination;
        boolean isDurable;
        long tte = -1;
        Buffer attachment;

        /**
         * @return the name.
         */
        public AsciiBuffer getName() {
            return name;
        }

        /**
         * @param name
         *            the name to set
         */
        public void setName(AsciiBuffer name) {
            this.name = name;
        }

        /**
         * @return the selector
         */
        public AsciiBuffer getSelector() {
            return selector;
        }

        /**
         * @param selector
         *            the selector to set.
         */
        public void setSelector(AsciiBuffer selector) {
            this.selector = selector;
        }

        /**
         * @return the destination
         */
        public AsciiBuffer getDestination() {
            return destination;
        }

        /**
         * @param destination
         *            the destination to set
         */
        public void setDestination(AsciiBuffer destination) {
            this.destination = destination;
        }

        /**
         * @return the isDurable
         */
        public boolean getIsDurable() {
            return isDurable;
        }

        /**
         * @param isDurable
         *            the isDurable to set
         */
        public void setIsDurable(boolean isDurable) {
            this.isDurable = isDurable;
        }

        /**
         * @return the tte
         */
        public long getTte() {
            return tte;
        }

        /**
         * @param tte
         *            the tte to set
         */
        public void setTte(long tte) {
            this.tte = tte;
        }

        /**
         * @return the attachment
         */
        public Buffer getAttachment() {
            return attachment;
        }

        /**
         * @param attachment
         *            the attachment to set
         */
        public void setAttachment(Buffer attachment) {
            this.attachment = attachment;
        }

        public int hashCode() {
            return name.hashCode();
        }

        public boolean equals(Object o) {
            if (o instanceof SubscriptionRecord) {
                return equals((SubscriptionRecord) o);
            } else {
                return false;
            }
        }

        public boolean equals(SubscriptionRecord r) {
            if (r == null) {
                return false;
            }
            if (r.hashCode() != hashCode())
                return false;

            if (r.isDurable != isDurable)
                return false;

            if (r.tte != tte)
                return false;

            if (!name.equals(r.name))
                return false;

            if (!destination.equals(r.destination))
                return false;
            
            if (!(selector == null ? r.selector == null : selector.equals(r.selector))) 
                return false;
            
            if (!(attachment == null ? r.attachment == null : attachment.equals(r.attachment))) 
                return false;
            
            return true;
        }
    }

    public static class QueueRecord {
        Long queueKey;
        Long messageKey;
        Buffer attachment;
        int size;
        boolean redelivered;
        long tte;

        public boolean isRedelivered() {
            return redelivered;
        }

        public void setRedelivered(boolean redelivered) {
            this.redelivered = redelivered;
        }

        public long getTte() {
            return tte;
        }

        public void setTte(long tte) {
            this.tte = tte;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public Long getQueueKey() {
            return queueKey;
        }

        public void setQueueKey(Long queueKey) {
            this.queueKey = queueKey;
        }

        public Long getMessageKey() {
            return messageKey;
        }

        public void setMessageKey(Long messageKey) {
            this.messageKey = messageKey;
        }

        public Buffer getAttachment() {
            return attachment;
        }

        public void setAttachment(Buffer attachment) {
            this.attachment = attachment;
        }
    }

    // Message related methods.
    public static class MessageRecord {
        Long key = (long) -1;
        AsciiBuffer messageId;
        AsciiBuffer encoding;
        int size;
        Buffer buffer;
        Long streamKey;

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
        
        public Long getKey() {
            return key;
        }

        public void setKey(Long key) {
            this.key = key;
        }

        public AsciiBuffer getMessageId() {
            return messageId;
        }

        public void setMessageId(AsciiBuffer messageId) {
            this.messageId = messageId;
        }

        public AsciiBuffer getEncoding() {
            return encoding;
        }

        public void setEncoding(AsciiBuffer encoding) {
            this.encoding = encoding;
        }

        public Buffer getBuffer() {
            return buffer;
        }

        public void setBuffer(Buffer buffer) {
            this.buffer = buffer;
        }

        public Long getStreamKey() {
            return streamKey;
        }

        public void setStreamKey(Long stream) {
            this.streamKey = stream;
        }
    }

    /**
     * Result Holder for queue related queries.
     */
    public interface QueueQueryResult {

        /**
         * @return the descriptor for the queue.
         */
        public QueueDescriptor getDescriptor();

        /**
         * Gets the count of elements in this queue. Note that this does not
         * include counts for elements held in child partitions.
         * 
         * @return the number of elements in the queue.
         */
        public int getCount();

        /**
         * Gets the size of elements in this queue. Note that this does not
         * include size of elements held in child partitions.
         * 
         * @return the total size of elements in the queue
         */
        public long getSize();

        /**
         * @return the first sequence number in the queue.
         */
        public long getFirstSequence();

        /**
         * @return the last sequence number in the queue.
         */
        public long getLastSequence();

        /**
         * @return The results for this queue's partitions
         */
        public Collection<QueueQueryResult> getPartitions();
    }

    /**
     * Executes user supplied {@link Callback}. If the {@link Callback} does not
     * throw any Exceptions, all updates to the store are committed to the store
     * as a single unit of work, otherwise they are rolled back.
     * 
     * When this method returns, the transaction may be buffered by the Store
     * implementation it increase performance throughput. The onFlush parameter
     * can be used to know when the transaction does get flushed is guaranteed
     * to not be lost if a system crash occurs.
     * 
     * You can force the flushing of all previously buffered transactions using
     * the {@link #flush} method.
     * 
     * Any exceptions thrown by the {@link Callback} are propagated by this
     * method.
     * 
     * @param <T>
     * @param closure
     * @param onFlush
     *            if not null, it's {@link Runnable#run()} method is called once
     *            he transaction has been store on disk.
     */
    public <R, T extends Exception> R execute(Callback<R, T> callback, Runnable onFlush) throws T, FatalStoreException;

    /**
     * Flushes all committed buffered transactions.
     */
    public void flush();

    /**
     * @return true if the store is transactional.
     */
    public boolean isTransactional();

    /**
     * This interface allows you to query and update the Store.
     * 
     * This interface should only be called within the context of a transaction
     * controlled by the {@link Store#execute(Callback)} mehtod.
     * 
     */
    public interface Session {

        ////////////////////////////////////////////////////////////////
        //Lock related methods:
        ////////////////////////////////////////////////////////////////

        /**
         * Commits work done on the Session
         */
        public void commit();

        /**
         * Rolls back work done on the Session since the last call to
         * {@link #acquireLock()}
         * 
         * @throw {@link UnsupportedOperationException} if the store is not
         *        transactional
         */
        public void rollback();

        /**
         * Indicates callers intent to start a transaction.
         */
        public void acquireLock();

        /**
         * Indicates caller is done with the transaction, if not committed then
         * the transaction will be rolled back (providing the store is
         * transactional.
         */
        public void releaseLock();

        ////////////////////////////////////////////////////////////////
        //Client related methods
        ////////////////////////////////////////////////////////////////

        /**
         * Adds a subscription to the store.
         * 
         * @throws DuplicateKeyException
         *             if a subscription with the same name already exists
         * 
         */
        public void addSubscription(SubscriptionRecord record) throws DuplicateKeyException;

        /**
         * Updates a subscription in the store. If the subscription does not
         * exist then it will simply be added.
         */
        public void updateSubscription(SubscriptionRecord record);

        /**
         * Removes a subscription with the given name from the store.
         */
        public void removeSubscription(AsciiBuffer name);

        /**
         * @return A list of the stored subscriptions.
         */
        public Iterator<SubscriptionRecord> listSubscriptions();

        ////////////////////////////////////////////////////////////////
        //Queue related methods
        ////////////////////////////////////////////////////////////////
        /**
         * Gets a list of queues. The returned iterator returns top-level queues
         * (e.g. queues without a parent). The child queues are accessible via
         * {@link QueueQueryResult#getPartitions()}.
         * 
         * @param firstQueueName
         *            If null starts the query at the first queue.
         * @param max
         *            The maximum number of queues to return
         * @return The list of queues.
         */
        public Iterator<QueueQueryResult> queueList(QueueDescriptor firstQueueName, int max);

        /**
         * Gets a list of queues for which
         * {@link QueueDescriptor#getQueueType()} matches the specified type.
         * The returned iterator returns top-level queues (e.g. queues without a
         * parent). The child queues are accessible via
         * {@link QueueQueryResult#getPartitions()}.
         * 
         * @param firstQueueName
         *            If null starts the query at the first queue.
         * @param max
         *            The maximum number of queues to return
         * @param type
         *            The type of queue to consider
         * @return The list of queues.
         */
        public Iterator<QueueQueryResult> queueListByType(short type, QueueDescriptor firstQueueName, int max);

        /**
         * Adds a queue. If {@link QueueDescriptor#getParent()} is specified
         * then the parent queue must exist.
         * 
         * @param queue
         *            The queue to add.
         * 
         * @throws KeyNotFoundException
         *             if the descriptor specifies a non existent parent
         */
        public void queueAdd(QueueDescriptor queue) throws KeyNotFoundException;

        /**
         * Deletes a queue and all of it's messages. If it has any child
         * partitions they are deleted as well.
         * 
         * @param queue
         *            The queue to delete
         */
        public void queueRemove(QueueDescriptor queue);

        /**
         * Adds a reference to the message for the given queue. The associated
         * queue record contains the sequence number of the message in this
         * queue and the store tracking number of the associated message.
         * 
         * @param queue
         *            The queue descriptor
         * @param record
         *            The queue record
         * @throws KeyNotFoundException
         *             If there is no message associated with
         *             {@link QueueRecord#getMessageKey()}
         */
        public void queueAddMessage(QueueDescriptor queue, QueueRecord record) throws KeyNotFoundException;

        /**
         * Deletes an element from a queue.
         * @param queue The queue
         * @param queueKey The element's queue key as given by {@link QueueRecord#setQueueKey(Long)}
         * @throws KeyNotFoundException
         */
        public void queueRemoveMessage(QueueDescriptor queue, Long queueKey) throws KeyNotFoundException;

        public Iterator<QueueRecord> queueListMessagesQueue(QueueDescriptor queue, Long firstQueueKey, Long maxSequence, int max) throws KeyNotFoundException;

        ////////////////////////////////////////////////////////////////
        //Message related methods
        ////////////////////////////////////////////////////////////////
        public void messageAdd(MessageRecord message);

        public MessageRecord messageGetRecord(Long key) throws KeyNotFoundException;

        public Long streamOpen();

        public void streamWrite(Long streamKey, Buffer message) throws KeyNotFoundException;

        public void streamClose(Long streamKey) throws KeyNotFoundException;

        public Buffer streamRead(Long streamKey, int offset, int max) throws KeyNotFoundException;

        public boolean streamRemove(Long streamKey);

        ////////////////////////////////////////////////////////////////
        //Transaction related methods 
        //TODO these will probably go away in favor of just using a queue.
        ////////////////////////////////////////////////////////////////
        public Iterator<Buffer> transactionList(Buffer first, int max);

        public void transactionAdd(Buffer txid);

        public void transactionAddMessage(Buffer txid, Long messageKey) throws KeyNotFoundException;

        public void transactionRemoveMessage(Buffer txid, QueueDescriptor queueName, Long messageKey) throws KeyNotFoundException;

        public void transactionCommit(Buffer txid) throws KeyNotFoundException;

        public void transactionRollback(Buffer txid) throws KeyNotFoundException;

        ////////////////////////////////////////////////////////////////
        //Map related methods 
        ////////////////////////////////////////////////////////////////

        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max);

        public void mapAdd(AsciiBuffer map);

        public void mapRemove(AsciiBuffer map);

        public void mapEntryPut(AsciiBuffer map, AsciiBuffer key, Buffer value) throws KeyNotFoundException;

        public Buffer mapEntryGet(AsciiBuffer map, AsciiBuffer key) throws KeyNotFoundException;

        public void mapEntryRemove(AsciiBuffer map, AsciiBuffer key) throws KeyNotFoundException;

        public Iterator<AsciiBuffer> mapEntryListKeys(AsciiBuffer map, AsciiBuffer first, int max) throws KeyNotFoundException;

    }
}
