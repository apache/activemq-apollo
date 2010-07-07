package org.apache.activemq.broker.store.hawtdb.store;

import org.apache.activemq.apollo.store.*;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdb.api.Transaction;
import org.fusesource.hawtdb.internal.journal.Location;

import java.io.IOException;
import java.util.Iterator;

/**
* Created by IntelliJ IDEA.
* User: chirino
* Date: May 19, 2010
* Time: 4:51:30 PM
* To change this template use File | Settings | File Templates.
*/
class HawtDBSession {

    Data.Type.TypeCreatable atomicUpdate = null;
    int updateCount = 0;

    private Transaction tx;
    private HawtDBManager store;

    public HawtDBSession(HawtDBManager store) {
        this.store = store;
    }

    private Transaction tx() {
        acquireLock();
        return tx;
    }

    public final void commit() {
        commit(null);
    }

    public final void rollback() {
        try {
            if (tx != null) {
                if (updateCount > 1) {
                    store.journal.write(HawtDBManager.CANCEL_UNIT_OF_WORK_DATA, false);
                }
                tx.rollback();
            } else {
                throw new IllegalStateException("Not in Transaction");
            }
        } catch (IOException e) {
            throw new FatalStoreException(e);
        } finally {
            if (tx != null) {
                tx = null;
                updateCount = 0;
                atomicUpdate = null;
            }
        }
    }

    /**
     * Indicates callers intent to start a transaction.
     */
    public final void acquireLock() {
        if (tx == null) {
            store.indexLock.writeLock().lock();
            tx = store.pageFile.tx();
        }
    }

    public final void releaseLock() {
        try {
            if (tx != null) {
                rollback();
            }
        } finally {
            store.indexLock.writeLock().unlock();
        }
    }

    public void commit(Runnable onFlush) {
        try {

            boolean flush = false;
            if (atomicUpdate != null) {
                store.store(atomicUpdate, onFlush, tx);
            } else if (updateCount > 1) {
                store.journal.write(HawtDBManager.END_UNIT_OF_WORK_DATA, onFlush);
            } else {
                flush = onFlush != null;
            }

            if (tx != null) {
                tx.commit();
            }

            if (flush) {
                onFlush.run();
            }

        } catch (IOException e) {
            throw new FatalStoreException(e);
        } finally {
            tx = null;
            updateCount = 0;
            atomicUpdate = null;
        }
    }

    private void storeAtomic() {
        if (atomicUpdate != null) {
            try {
                store.journal.write(HawtDBManager.BEGIN_UNIT_OF_WORK_DATA, false);
                store.store(atomicUpdate, null, tx);
                atomicUpdate = null;
            } catch (IOException ioe) {
                throw new FatalStoreException(ioe);
            }
        }
    }

    private void addUpdate(Data.Type.TypeCreatable bean) {
        try {
            //As soon as we do more than one update we'll wrap in a unit of
            //work:
            if (updateCount == 0) {
                atomicUpdate = bean;
                updateCount++;
                return;
            }
            storeAtomic();

            updateCount++;
            store.store(bean, null, tx);

        } catch (IOException ioe) {
            throw new FatalStoreException(ioe);
        }
    }

    // /////////////////////////////////////////////////////////////
    // Message related methods.
    // /////////////////////////////////////////////////////////////

    public void messageAdd(MessageRecord message) {
        if (message.id < 0) {
            throw new IllegalArgumentException("Key not set");
        }
        Data.MessageAdd.MessageAddBean bean = new Data.MessageAdd.MessageAddBean();
        bean.setMessageKey(message.id);
        bean.setMessageId(message.messageId);
        bean.setProtocol(message.protocol);
        bean.setMessageSize(message.size);
        Buffer buffer = message.value;
        if (buffer != null) {
            bean.setValue(buffer);
        }
        Long streamKey = message.stream;
        if (streamKey != null) {
            bean.setStreamKey(streamKey);
        }

        addUpdate(bean);
    }

    public MessageRecord messageGetRecord(Long key) throws KeyNotFoundException {
        storeAtomic();
        Location location = store.rootEntity.messageGetLocation(tx(), key);
        if (location == null) {
            throw new KeyNotFoundException("message key: " + key);
        }
        try {
            Data.MessageAdd bean = (Data.MessageAdd) store.load(location);
            MessageRecord rc = new MessageRecord();
            rc.id = bean.getMessageKey();
            rc.messageId = bean.getMessageId();
            rc.protocol = bean.getProtocol();
            rc.size = bean.getMessageSize();
            if (bean.hasValue()) {
                rc.value = bean.getValue();
            }
            if (bean.hasStreamKey()) {
                rc.stream = bean.getStreamKey();
            }
            return rc;
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    // /////////////////////////////////////////////////////////////
    // Queue related methods.
    // /////////////////////////////////////////////////////////////
    public void queueAdd(QueueRecord record) {
        Data.QueueAdd.QueueAddBean update = new Data.QueueAdd.QueueAddBean();
        update.setName(record.name);
        update.setQueueType(record.queueType);
//        AsciiBuffer parent = record.getParent();
//        if (parent != null) {
//            update.setParentName(parent);
//            update.setPartitionId(record.getPartitionKey());
//        }
        addUpdate(update);
    }

    public void queueRemove(QueueRecord record) {
        addUpdate(new Data.QueueRemove.QueueRemoveBean().setKey(record.id));
    }

    public Iterator<QueueStatus> queueListByType(AsciiBuffer type, QueueRecord firstQueue, int max) {
        storeAtomic();
        try {
            return store.rootEntity.queueList(tx(), type, firstQueue, max);
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    public Iterator<QueueStatus> queueList(QueueRecord firstQueue, int max) {
        storeAtomic();
        try {
            return store.rootEntity.queueList(tx(), null, firstQueue, max);
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    public void queueAddMessage(QueueRecord queue, QueueEntryRecord entryRecord) throws KeyNotFoundException {
        Data.QueueAddMessage.QueueAddMessageBean bean = new Data.QueueAddMessage.QueueAddMessageBean();
        bean.setQueueKey(queue.id);
        bean.setQueueKey(entryRecord.queueKey);
        bean.setMessageKey(entryRecord.messageKey);
        bean.setMessageSize(entryRecord.size);
        if (entryRecord.attachment != null) {
            bean.setAttachment(entryRecord.attachment);
        }
        addUpdate(bean);
    }

    public void queueRemoveMessage(QueueRecord queue, Long queueKey) throws KeyNotFoundException {
        Data.QueueRemoveMessage.QueueRemoveMessageBean bean = new Data.QueueRemoveMessage.QueueRemoveMessageBean();
        bean.setQueueKey(queueKey);
        bean.setQueueName(queue.name);
        addUpdate(bean);
    }

    public Iterator<QueueEntryRecord> queueListMessagesQueue(QueueRecord queue, Long firstQueueKey, Long maxQueueKey, int max) throws KeyNotFoundException {
        storeAtomic();
        DestinationEntity destination = store.rootEntity.getDestination(queue.id);
        if (destination == null) {
            throw new KeyNotFoundException("queue key: " + queue);
        }
        try {
            return destination.listMessages(tx(), firstQueueKey, maxQueueKey, max);
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

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
    public void addSubscription(SubscriptionRecord record) throws DuplicateKeyException {
        storeAtomic();
        SubscriptionRecord old;
        try {
            old = store.rootEntity.getSubscription(record.name);
            if (old != null && !old.equals(record)) {
                throw new DuplicateKeyException("Subscription already exists: " + record.name);
            } else {
                updateSubscription(record);
            }
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    /**
     * Updates a subscription in the store. If the subscription does not
     * exist then it will simply be added.
     */
    public void updateSubscription(SubscriptionRecord record) {
        Data.SubscriptionAdd.SubscriptionAddBean update = new Data.SubscriptionAdd.SubscriptionAddBean();
        update.setName(record.name);
        update.setDestination(record.destination);
        update.setDurable(record.isDurable);

        if (record.attachment != null) {
            update.setAttachment(record.attachment);
        }
        if (record.selector != null) {
            update.setSelector(record.selector);
        }
        if (record.expiration != -1) {
            update.setTte(record.expiration);
        }
        addUpdate(update);
    }

    /**
     * Removes a subscription with the given name from the store.
     */
    public void removeSubscription(AsciiBuffer name) {
        Data.SubscriptionRemove.SubscriptionRemoveBean update = new Data.SubscriptionRemove.SubscriptionRemoveBean();
        update.setName(name);
        addUpdate(update);
    }

    /**
     * @return A list of subscriptions
     */
    public Iterator<SubscriptionRecord> listSubscriptions() {
        storeAtomic();
        try {
            return store.rootEntity.listSubsriptions(tx);
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    // /////////////////////////////////////////////////////////////
    // Map related methods.
    // /////////////////////////////////////////////////////////////
    public void mapAdd(AsciiBuffer map) {
        Data.MapAdd.MapAddBean update = new Data.MapAdd.MapAddBean();
        update.setMapName(map);
        addUpdate(update);
    }

    public void mapRemove(AsciiBuffer map) {
        Data.MapRemove.MapRemoveBean update = new Data.MapRemove.MapRemoveBean();
        update.setMapName(map);
        addUpdate(update);
    }

    public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max) {
        storeAtomic();
        return store.rootEntity.mapList(first, max, tx);
    }

    public void mapEntryPut(AsciiBuffer map, AsciiBuffer key, Buffer value) {
        Data.MapEntryPut.MapEntryPutBean update = new Data.MapEntryPut.MapEntryPutBean();
        update.setMapName(map);
        update.setId(key);
        update.setValue(value);
        addUpdate(update);
    }

    public Buffer mapEntryGet(AsciiBuffer map, AsciiBuffer key) throws KeyNotFoundException {
        storeAtomic();
        try {
            return store.rootEntity.mapGetEntry(map, key, tx);
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    public void mapEntryRemove(AsciiBuffer map, AsciiBuffer key) throws KeyNotFoundException {
        Data.MapEntryRemove.MapEntryRemoveBean update = new Data.MapEntryRemove.MapEntryRemoveBean();
        update.setMapName(map);
        update.setId(key);
        addUpdate(update);
    }

    public Iterator<AsciiBuffer> mapEntryListKeys(AsciiBuffer map, AsciiBuffer first, int max) throws KeyNotFoundException {
        storeAtomic();
        try {
            return store.rootEntity.mapListKeys(map, first, max, tx);
        } catch (IOException e) {
            throw new FatalStoreException(e);
        }
    }

    // /////////////////////////////////////////////////////////////
    // Stream related methods.
    // /////////////////////////////////////////////////////////////
    public Long streamOpen() {
        return null;
    }

    public void streamWrite(Long streamKey, Buffer message) throws KeyNotFoundException {
    }

    public void streamClose(Long streamKey) throws KeyNotFoundException {
    }

    public Buffer streamRead(Long streamKey, int offset, int max) throws KeyNotFoundException {
        return null;
    }

    public boolean streamRemove(Long streamKey) {
        return false;
    }

    // /////////////////////////////////////////////////////////////
    // Transaction related methods.
    // /////////////////////////////////////////////////////////////
    public void transactionAdd(Buffer txid) {
    }

    public void transactionAddMessage(Buffer txid, Long messageKey) throws KeyNotFoundException {
    }

    public void transactionCommit(Buffer txid) throws KeyNotFoundException {
    }

    public Iterator<Buffer> transactionList(Buffer first, int max) {
        return null;
    }

    public void transactionRemoveMessage(Buffer txid, QueueRecord queueName, Long messageKey) throws KeyNotFoundException {
    }

    public void transactionRollback(Buffer txid) throws KeyNotFoundException {
    }
}
