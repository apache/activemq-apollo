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
package org.apache.activemq.broker.store.kahadb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.kahadb.Data.MessageAdd;
import org.apache.activemq.broker.store.kahadb.Data.QueueAdd;
import org.apache.activemq.broker.store.kahadb.Data.QueueAddMessage;
import org.apache.activemq.broker.store.kahadb.Data.QueueRemove;
import org.apache.activemq.broker.store.kahadb.Data.QueueRemoveMessage;
import org.apache.activemq.broker.store.kahadb.Data.Trace;
import org.apache.activemq.broker.store.kahadb.Data.Type;
import org.apache.activemq.broker.store.kahadb.Data.MessageAdd.MessageAddBean;
import org.apache.activemq.broker.store.kahadb.Data.QueueAdd.QueueAddBean;
import org.apache.activemq.broker.store.kahadb.Data.QueueAddMessage.QueueAddMessageBean;
import org.apache.activemq.broker.store.kahadb.Data.QueueRemove.QueueRemoveBean;
import org.apache.activemq.broker.store.kahadb.Data.QueueRemoveMessage.QueueRemoveMessageBean;
import org.apache.activemq.broker.store.kahadb.Data.Type.TypeCreatable;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.InvalidProtocolBufferException;
import org.apache.activemq.protobuf.MessageBuffer;
import org.apache.activemq.protobuf.PBMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.journal.Journal;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayInputStream;
import org.apache.kahadb.util.DataByteArrayOutputStream;
import org.apache.kahadb.util.LockFile;

public class KahaDBStore implements Store {

    private static final int BEGIN_UNIT_OF_WORK = -1;
    private static final int END_UNIT_OF_WORK = -2;
    private static final int FLUSH = -3;

    private static final Log LOG = LogFactory.getLog(KahaDBStore.class);
    private static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    private static final ByteSequence BEGIN_UNIT_OF_WORK_DATA = new ByteSequence(new byte[] { END_UNIT_OF_WORK });
    private static final ByteSequence END_UNIT_OF_WORK_DATA = new ByteSequence(new byte[] { END_UNIT_OF_WORK });
    private static final ByteSequence FLUSH_DATA = new ByteSequence(new byte[] { FLUSH });

    public static final int CLOSED_STATE = 1;
    public static final int OPEN_STATE = 2;

    protected PageFile pageFile;
    protected Journal journal;

    protected RootEntity rootEntity = new RootEntity();

    protected boolean failIfDatabaseIsLocked;
    protected boolean deleteAllMessages;
    protected File directory;
    protected Thread checkpointThread;
    protected boolean enableJournalDiskSyncs = true;
    long checkpointInterval = 5 * 1000;
    long cleanupInterval = 30 * 1000;
    int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    boolean enableIndexWriteAsync = false;
    int setIndexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;

    protected AtomicBoolean started = new AtomicBoolean();
    protected AtomicBoolean opened = new AtomicBoolean();
    private LockFile lockFile;
    private Location nextRecoveryPosition;
    private Location lastRecoveryPosition;
    private AtomicLong trackingGen = new AtomicLong(0);

    protected final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();
    private final HashSet<Integer> journalFilesBeingReplicated = new HashSet<Integer>();

    private static class UoWOperation {
        public TypeCreatable bean;
        public Location location;
        public ByteSequence data;
    }

    // /////////////////////////////////////////////////////////////////
    // Lifecylce methods
    // /////////////////////////////////////////////////////////////////
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            load();
        }
    }

    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            unload();
        }
    }

    /**
     * @return a unique sequential store tracking number.
     */
    public long allocateStoreTracking() {
        return trackingGen.incrementAndGet();
    }

    private void loadPageFile() throws IOException {
        indexLock.writeLock().lock();
        try {
            final PageFile pageFile = getPageFile();
            pageFile.load();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    if (pageFile.getPageCount() == 0) {
                        rootEntity.allocate(tx);
                    } else {
                        Page<RootEntity> page = tx.load(0, RootEntity.MARSHALLER);
                        rootEntity = page.get();
                        rootEntity.setPageId(0);
                    }
                    rootEntity.load(tx);
                }
            });
            pageFile.flush();

        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * @throws IOException
     */
    public void open() throws IOException {
        if (opened.compareAndSet(false, true)) {
            if (directory == null) {
                throw new IllegalArgumentException("The directory property must be set.");
            }

            File lockFileName = new File(directory, "lock");
            lockFile = new LockFile(lockFileName, true);
            if (failIfDatabaseIsLocked) {
                lockFile.lock();
            } else {
                while (true) {
                    try {
                        lockFile.lock();
                        break;
                    } catch (IOException e) {
                        LOG.info("Database " + lockFileName + " is locked... waiting " + (DATABASE_LOCKED_WAIT_DELAY / 1000) + " seconds for the database to be unlocked.");
                        try {
                            Thread.sleep(DATABASE_LOCKED_WAIT_DELAY);
                        } catch (InterruptedException e1) {
                        }
                    }
                }
            }

            getJournal().start();

            loadPageFile();

            checkpointThread = new Thread("ActiveMQ Journal Checkpoint Worker") {
                public void run() {
                    try {
                        long lastCleanup = System.currentTimeMillis();
                        long lastCheckpoint = System.currentTimeMillis();

                        // Sleep for a short time so we can periodically check
                        // to see if we need to exit this thread.
                        long sleepTime = Math.min(checkpointInterval, 500);
                        while (opened.get()) {
                            Thread.sleep(sleepTime);
                            long now = System.currentTimeMillis();
                            if (now - lastCleanup >= cleanupInterval) {
                                checkpointCleanup(true);
                                lastCleanup = now;
                                lastCheckpoint = now;
                            } else if (now - lastCheckpoint >= checkpointInterval) {
                                checkpointCleanup(false);
                                lastCheckpoint = now;
                            }
                        }
                    } catch (InterruptedException e) {
                        // Looks like someone really wants us to exit this
                        // thread...
                    }
                }
            };
            checkpointThread.start();
            recover();
        }
    }

    public void load() throws IOException {
        indexLock.writeLock().lock();
        try {
            open();

            if (deleteAllMessages) {
                journal.delete();

                pageFile.unload();
                pageFile.delete();
                rootEntity = new RootEntity();

                LOG.info("Persistence store purged.");
                deleteAllMessages = false;

                loadPageFile();
            }
            store(new Trace.TraceBean().setMessage(new AsciiBuffer("LOADED " + new Date())));
        } finally {
            indexLock.writeLock().unlock();
        }

    }

    public void close() throws IOException, InterruptedException {
        if (opened.compareAndSet(true, false)) {

            indexLock.writeLock().lock();
            try {
                pageFile.unload();
                rootEntity = new RootEntity();
            } finally {
                indexLock.writeLock().unlock();
            }
            journal.close();
            checkpointThread.join();
            lockFile.unlock();
            lockFile = null;
        }
    }

    public void unload() throws IOException, InterruptedException {
        if (pageFile.isLoaded()) {
            indexLock.writeLock().lock();
            try {
                rootEntity.setState(CLOSED_STATE);
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        rootEntity.store(tx);
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
            close();
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Recovery methods
    // /////////////////////////////////////////////////////////////////

    /**
     * Move all the messages that were in the journal into long term storage. We
     * just replay and do a checkpoint.
     * 
     * @throws IOException
     * @throws IOException
     * @throws InvalidLocationException
     * @throws IllegalStateException
     */
    private void recover() throws IllegalStateException, IOException {
        indexLock.writeLock().lock();
        try {
            long start = System.currentTimeMillis();

            ArrayList<UoWOperation> uow = null;
            Location recoveryPosition = getRecoveryPosition();
            if (recoveryPosition != null) {
                int redoCounter = 0;
                while (recoveryPosition != null) {

                    ByteSequence data = journal.read(recoveryPosition);
                    if (data.length == 1 && data.data[0] == BEGIN_UNIT_OF_WORK) {
                        uow = new ArrayList<UoWOperation>();
                    } else if (data.length == 1 && data.data[0] == END_UNIT_OF_WORK) {
                        if (uow != null) {
                            final ArrayList<UoWOperation> list = uow;
                            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                                public void execute(Transaction tx) throws IOException {
                                    for (UoWOperation op : list) {
                                        updateIndex(tx, op.bean.toType(), (MessageBuffer) op.bean, op.location);
                                        rootEntity.setLastUpdate(op.location);
                                    }
                                }
                            });
                            redoCounter += uow.size();
                            uow = null;
                        }
                    } else if (data.length == 1 && data.data[0] == FLUSH) {
                    } else {
                        final TypeCreatable message = load(recoveryPosition);
                        final Location location = recoveryPosition;
                        if (uow != null) {
                            UoWOperation op = new UoWOperation();
                            op.bean = message;
                            op.data = data;
                            op.location = recoveryPosition;
                            uow.add(op);
                        } else {
                            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                                public void execute(Transaction tx) throws IOException {
                                    updateIndex(tx, message.toType(), (MessageBuffer) message, location);
                                    rootEntity.setLastUpdate(location);
                                }
                            });
                            redoCounter++;
                        }
                    }

                    recoveryPosition = journal.getNextLocation(recoveryPosition);
                }
                long end = System.currentTimeMillis();
                LOG.info("Replayed " + redoCounter + " operations from the journal in " + ((end - start) / 1000.0f) + " seconds.");
            }

            // We may have to undo some index updates.
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    recoverIndex(tx);
                }
            });
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void incrementalRecover() throws IOException {
        indexLock.writeLock().lock();
        try {
            if (nextRecoveryPosition == null) {
                if (lastRecoveryPosition == null) {
                    nextRecoveryPosition = getRecoveryPosition();
                } else {
                    nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
                }
            }
            while (nextRecoveryPosition != null) {
                lastRecoveryPosition = nextRecoveryPosition;
                rootEntity.setLastUpdate(lastRecoveryPosition);
                final TypeCreatable message = load(lastRecoveryPosition);
                final Location location = lastRecoveryPosition;

                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, message.toType(), (MessageBuffer) message, location);
                    }
                });

                nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    protected void recoverIndex(Transaction tx) throws IOException {
        long start = System.currentTimeMillis();
        // It is possible index updates got applied before the journal updates..
        // in that case we need to removed references to messages that are not
        // in the journal
        final Location lastAppendLocation = journal.getLastAppendLocation();
        long undoCounter = 0;

        // TODO
        // // Go through all the destinations to see if they have messages past
        // the lastAppendLocation
        // for (StoredDestinationState sd : storedDestinations.values()) {
        //        	
        // final ArrayList<Long> matches = new ArrayList<Long>();
        // // Find all the Locations that are >= than the last Append Location.
        // sd.locationIndex.visit(tx, new BTreeVisitor.GTEVisitor<Location,
        // Long>(lastAppendLocation) {
        // @Override
        // protected void matched(Location key, Long value) {
        // matches.add(value);
        // }
        // });
        //            
        //            
        // for (Long sequenceId : matches) {
        // MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
        // sd.locationIndex.remove(tx, keys.location);
        // sd.messageIdIndex.remove(tx, keys.messageId);
        // undoCounter++;
        // // TODO: do we need to modify the ack positions for the pub sub case?
        // }
        // }
        long end = System.currentTimeMillis();
        if (undoCounter > 0) {
            // The rolledback operations are basically in flight journal writes.
            // To avoid getting these the end user
            // should do sync writes to the journal.
            LOG.info("Rolled back " + undoCounter + " operations from the index in " + ((end - start) / 1000.0f) + " seconds.");
        }
    }

    public Location getLastUpdatePosition() throws IOException {
        return rootEntity.getLastUpdate();
    }

    private Location getRecoveryPosition() throws IOException {

        if (rootEntity.getLastUpdate() != null) {
            // Start replay at the record after the last one recorded in the
            // index file.
            return journal.getNextLocation(rootEntity.getLastUpdate());
        }

        // This loads the first position.
        return journal.getNextLocation(null);
    }

    protected void checkpointCleanup(final boolean cleanup) {
        try {
            long start = System.currentTimeMillis();
            indexLock.writeLock().lock();
            try {
                if (!opened.get()) {
                    return;
                }
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        checkpointUpdate(tx, cleanup);
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
            long end = System.currentTimeMillis();
            if (end - start > 100) {
                LOG.warn("KahaDB Cleanup took " + (end - start));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void checkpoint(org.apache.activemq.util.Callback closure) throws Exception {
        indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    checkpointUpdate(tx, false);
                }
            });
            closure.execute();
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * @param tx
     * @throws IOException
     */
    private void checkpointUpdate(Transaction tx, boolean cleanup) throws IOException {

        LOG.debug("Checkpoint started.");

        rootEntity.setState(OPEN_STATE);
        rootEntity.store(tx);
        pageFile.flush();

        if (cleanup) {

            final TreeSet<Integer> gcCandidateSet = new TreeSet<Integer>(journal.getFileMap().keySet());

            // Don't GC files under replication
            if (journalFilesBeingReplicated != null) {
                gcCandidateSet.removeAll(journalFilesBeingReplicated);
            }

            // Don't GC files after the first in progress tx
            Location firstTxLocation = rootEntity.getLastUpdate();

            if (firstTxLocation != null) {
                while (!gcCandidateSet.isEmpty()) {
                    Integer last = gcCandidateSet.last();
                    if (last >= firstTxLocation.getDataFileId()) {
                        gcCandidateSet.remove(last);
                    } else {
                        break;
                    }
                }
            }

            // // Go through all the destinations to see if any of them can
            // remove GC candidates.
            // for (StoredDestinationState sd : storedDestinations.values()) {
            // if( gcCandidateSet.isEmpty() ) {
            // break;
            // }
            //                
            // // Use a visitor to cut down the number of pages that we load
            // dbstate.locationIndex.visit(tx, new BTreeVisitor<Location,
            // Long>() {
            // int last=-1;
            // public boolean isInterestedInKeysBetween(Location first, Location
            // second) {
            // if( first==null ) {
            // SortedSet<Integer> subset =
            // gcCandidateSet.headSet(second.getDataFileId()+1);
            // if( !subset.isEmpty() && subset.last() == second.getDataFileId()
            // ) {
            // subset.remove(second.getDataFileId());
            // }
            // return !subset.isEmpty();
            // } else if( second==null ) {
            // SortedSet<Integer> subset =
            // gcCandidateSet.tailSet(first.getDataFileId());
            // if( !subset.isEmpty() && subset.first() == first.getDataFileId()
            // ) {
            // subset.remove(first.getDataFileId());
            // }
            // return !subset.isEmpty();
            // } else {
            // SortedSet<Integer> subset =
            // gcCandidateSet.subSet(first.getDataFileId(),
            // second.getDataFileId()+1);
            // if( !subset.isEmpty() && subset.first() == first.getDataFileId()
            // ) {
            // subset.remove(first.getDataFileId());
            // }
            // if( !subset.isEmpty() && subset.last() == second.getDataFileId()
            // ) {
            // subset.remove(second.getDataFileId());
            // }
            // return !subset.isEmpty();
            // }
            // }
            //    
            // public void visit(List<Location> keys, List<Long> values) {
            // for (Location l : keys) {
            // int fileId = l.getDataFileId();
            // if( last != fileId ) {
            // gcCandidateSet.remove(fileId);
            // last = fileId;
            // }
            // }
            // }
            //    
            // });
            // }

            if (!gcCandidateSet.isEmpty()) {
                LOG.debug("Cleanup removing the data files: " + gcCandidateSet);
                journal.removeDataFiles(gcCandidateSet);
            }
        }

        LOG.debug("Checkpoint done.");
    }

    public HashSet<Integer> getJournalFilesBeingReplicated() {
        return journalFilesBeingReplicated;
    }

    // /////////////////////////////////////////////////////////////////
    // Store interface
    // /////////////////////////////////////////////////////////////////
    long messageSequence;

    public Location store(TypeCreatable data) throws IOException {
        return store(data, null);
    }

    /**
     * All updated are are funneled through this method. The updates a converted
     * to a PBMessage which is logged to the journal and then the data from the
     * PBMessage is used to update the index just like it would be done during a
     * recovery process.
     * 
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public Location store(final TypeCreatable data, Runnable onFlush) throws IOException {
        final MessageBuffer message = ((PBMessage) data).freeze();
        int size = message.serializedSizeUnframed();
        DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
        os.writeByte(data.toType().getNumber());
        message.writeUnframed(os);

        long start = System.currentTimeMillis();
        final Location location;
        synchronized (journal) {
            location = journal.write(os.toByteSequence(), onFlush);
        }
        long start2 = System.currentTimeMillis();

        try {
            indexLock.writeLock().lock();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, data.toType(), message, location);
                }
            });
            rootEntity.setLastUpdate(location);
        } finally {
            indexLock.writeLock().unlock();
        }

        long end = System.currentTimeMillis();
        if (end - start > 100) {
            LOG.warn("KahaDB long enqueue time: Journal Add Took: " + (start2 - start) + " ms, Index Update took " + (end - start2) + " ms");
        }
        return location;
    }

    public void store(List<TypeCreatable> batch) throws IOException {
        store(batch, null);
    }

    // ArrayList<TypeCreatable>
    /**
     * All updated are are funneled through this method. The updates a converted
     * to a PBMessage which is logged to the journal and then the data from the
     * PBMessage is used to update the index just like it would be done during a
     * recovery process.
     * 
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void store(final List<TypeCreatable> batch, Runnable onFlush) throws IOException {
        if (batch.isEmpty()) {
            return;
        }
        if (batch.size() == 1) {
            store(batch.get(0), onFlush);
            return;
        }

        final ArrayList<UoWOperation> uow = new ArrayList<UoWOperation>(batch.size());
        for (TypeCreatable bean : batch) {
            final MessageBuffer message = ((PBMessage) bean).freeze();
            int size = message.serializedSizeUnframed();
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
            os.writeByte(bean.toType().getNumber());
            message.writeUnframed(os);
            UoWOperation op = new UoWOperation();
            op.bean = bean;
            op.data = os.toByteSequence();
            uow.add(op);
        }

        long start = System.currentTimeMillis();
        synchronized (journal) {
            journal.write(BEGIN_UNIT_OF_WORK_DATA, false);
            for (UoWOperation op : uow) {
                op.location = journal.write(op.data, false);
            }
            journal.write(END_UNIT_OF_WORK_DATA, onFlush);
        }
        long start2 = System.currentTimeMillis();

        try {
            indexLock.writeLock().lock();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    for (UoWOperation op : uow) {
                        MessageBuffer message = ((PBMessage) op.bean).freeze();
                        updateIndex(tx, op.bean.toType(), message, op.location);
                        rootEntity.setLastUpdate(op.location);
                    }
                }
            });
        } finally {
            indexLock.writeLock().unlock();
        }

        long end = System.currentTimeMillis();
        if (end - start > 100) {
            LOG.warn("KahaDB long enqueue time: Journal Add Took: " + (start2 - start) + " ms, Index Update took " + (end - start2) + " ms");
        }
    }

    /**
     * Loads a previously stored PBMessage
     * 
     * @param location
     * @return
     * @throws IOException
     */
    public TypeCreatable load(Location location) throws IOException {
        ByteSequence data = journal.read(location);
        return load(location, data);
    }

    private TypeCreatable load(Location location, ByteSequence data) throws IOException, InvalidProtocolBufferException {
        DataByteArrayInputStream is = new DataByteArrayInputStream(data);
        byte readByte = is.readByte();
        Type type = Type.valueOf(readByte);
        if (type == null) {
            throw new IOException("Could not load journal record. Invalid location: " + location);
        }
        MessageBuffer message = type.parseUnframed(new Buffer(data.data, data.offset + 1, data.length - 1));
        return (TypeCreatable) message;
    }

    @SuppressWarnings("unchecked")
    public void updateIndex(Transaction tx, Type type, MessageBuffer command, Location location) throws IOException {
        switch (type) {
        case MESSAGE_ADD:
            messageAdd(tx, (MessageAdd) command, location);
            return;
        case QUEUE_ADD:
            queueAdd(tx, (QueueAdd) command, location);
            return;
        case QUEUE_REMOVE:
            queueRemove(tx, (QueueRemove) command, location);
            return;
        case QUEUE_ADD_MESSAGE:
            queueAddMessage(tx, (QueueAddMessage) command, location);
            return;
        case QUEUE_REMOVE_MESSAGE:
            queueRemoveMessage(tx, (QueueRemoveMessage) command, location);
            return;

        case TRANSACTION_BEGIN:
        case TRANSACTION_ADD_MESSAGE:
        case TRANSACTION_REMOVE_MESSAGE:
        case TRANSACTION_COMMIT:
        case TRANSACTION_ROLLBACK:
        case MAP_ADD:
        case MAP_REMOVE:
        case MAP_ENTRY_PUT:
        case MAP_ENTRY_REMOVE:
        case STREAM_OPEN:
        case STREAM_WRITE:
        case STREAM_CLOSE:
        case STREAM_REMOVE:
            throw new UnsupportedOperationException();
        }
    }

    private void messageAdd(Transaction tx, MessageAdd command, Location location) throws IOException {
        rootEntity.messageAdd(tx, command, location);
    }

    private void queueAdd(Transaction tx, QueueAdd command, Location location) throws IOException {
        rootEntity.queueAdd(tx, command.getQueueName());
    }

    private void queueRemove(Transaction tx, QueueRemove command, Location location) throws IOException {
        rootEntity.queueRemove(tx, command.getQueueName());
    }

    private void queueAddMessage(Transaction tx, QueueAddMessage command, Location location) throws IOException {
        DestinationEntity destination = rootEntity.getDestination(command.getQueueName());
        if (destination != null) {
            destination.add(tx, command);
            rootEntity.addMessageRef(tx, command.getQueueName(), command.getMessageKey());
        }
    }

    private void queueRemoveMessage(Transaction tx, QueueRemoveMessage command, Location location) throws IOException {
        DestinationEntity destination = rootEntity.getDestination(command.getQueueName());
        if (destination != null) {
            if (destination.remove(tx, command.getMessageKey())) {
                rootEntity.removeMessageRef(tx, command.getQueueName(), command.getMessageKey());
            }
        }
    }

    class KahaDBSession implements Session {
        ArrayList<TypeCreatable> updates = new ArrayList<TypeCreatable>();

        private Transaction tx;

        private Transaction tx() {
            if (tx == null) {
                indexLock.readLock().lock();
                tx = pageFile.tx();
            }
            return tx;
        }

        public void close() {
            try {
                if (tx != null) {
                    tx.rollback();
                }
            } catch (IOException e) {
                throw new FatalStoreException(e);
            } finally {
                if (tx != null) {
                    indexLock.readLock().unlock();
                    tx = null;
                }
            }
        }

        public void commit(Runnable onFlush) {
            try {
                if (tx != null) {
                    tx.commit();
                }
            } catch (IOException e) {
                throw new FatalStoreException(e);
            } finally {
                if (tx != null) {
                    indexLock.readLock().unlock();
                    tx = null;
                }
            }

            try {
                store(updates, onFlush);
            } catch (IOException e) {
                throw new FatalStoreException(e);
            }
        }

        // /////////////////////////////////////////////////////////////
        // Message related methods.
        // /////////////////////////////////////////////////////////////
        public Long messageAdd(MessageRecord message) {
            Long id = rootEntity.nextMessageKey();
            MessageAddBean bean = new MessageAddBean();
            bean.setMessageKey(id);
            bean.setMessageId(message.getMessageId());
            bean.setEncoding(message.getEncoding());
            Buffer buffer = message.getBuffer();
            if (buffer != null) {
                bean.setBuffer(buffer);
            }
            Long streamKey = message.getStreamKey();
            if (streamKey != null) {
                bean.setStreamKey(streamKey);
            }
            updates.add(bean);
            return id;
        }

        public MessageRecord messageGetRecord(Long key) throws KeyNotFoundException {
            Location location = rootEntity.messageGetLocation(tx(), key);
            if (location == null) {
                throw new KeyNotFoundException("message key: " + key);
            }
            try {
                MessageAdd bean = (MessageAdd) load(location);
                MessageRecord rc = new MessageRecord();
                rc.setKey(bean.getMessageKey());
                rc.setMessageId(bean.getMessageId());
                rc.setEncoding(bean.getEncoding());
                if (bean.hasBuffer()) {
                    rc.setBuffer(bean.getBuffer());
                }
                if (bean.hasStreamKey()) {
                    rc.setStreamKey(bean.getStreamKey());
                }
                return rc;
            } catch (IOException e) {
                throw new FatalStoreException(e);
            }
        }

        // /////////////////////////////////////////////////////////////
        // Queue related methods.
        // /////////////////////////////////////////////////////////////
        public void queueAdd(AsciiBuffer queueName) {
            updates.add(new QueueAddBean().setQueueName(queueName));
        }

        public void queueRemove(AsciiBuffer queueName) {
            updates.add(new QueueRemoveBean().setQueueName(queueName));
        }

        public Iterator<AsciiBuffer> queueList(AsciiBuffer firstQueueName, int max) {
            return rootEntity.queueList(tx(), firstQueueName, max);
        }

        public Long queueAddMessage(AsciiBuffer queueName, QueueRecord record) throws KeyNotFoundException {
            DestinationEntity destination = rootEntity.getDestination(queueName);
            if (destination == null) {
                throw new KeyNotFoundException("queue key: " + queueName);
            }
            Long queueKey = destination.nextQueueKey();
            QueueAddMessageBean bean = new QueueAddMessageBean();
            bean.setQueueName(queueName);
            bean.setQueueKey(queueKey);
            bean.setMessageKey(record.getMessageKey());
            if (record.getAttachment() != null) {
                bean.setAttachment(record.getAttachment());
            }
            updates.add(bean);
            return queueKey;
        }

        public void queueRemoveMessage(AsciiBuffer queueName, Long messageKey) throws KeyNotFoundException {
            QueueRemoveMessageBean bean = new QueueRemoveMessageBean();
            bean.setMessageKey(messageKey);
            bean.setQueueName(queueName);
            updates.add(bean);
        }

        public Iterator<QueueRecord> queueListMessagesQueue(AsciiBuffer queueName, Long firstQueueKey, int max) throws KeyNotFoundException {
            DestinationEntity destination = rootEntity.getDestination(queueName);
            if (destination == null) {
                throw new KeyNotFoundException("queue key: " + queueName);
            }
            try {
                return destination.listMessages(tx(), firstQueueKey, max);
            } catch (IOException e) {
                throw new FatalStoreException(e);
            }
        }

        // /////////////////////////////////////////////////////////////
        // Map related methods.
        // /////////////////////////////////////////////////////////////
        public boolean mapAdd(AsciiBuffer map) {
            return false;
        }

        public boolean mapRemove(AsciiBuffer map) {
            return false;
        }

        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max) {
            return null;
        }

        public Buffer mapEntryPut(AsciiBuffer map, AsciiBuffer key, Buffer value) throws KeyNotFoundException {
            return null;
        }

        public Buffer mapEntryGet(AsciiBuffer map, AsciiBuffer key) throws KeyNotFoundException {
            return null;
        }

        public Buffer mapEntryRemove(AsciiBuffer map, AsciiBuffer key) throws KeyNotFoundException {
            return null;
        }

        public Iterator<AsciiBuffer> mapEntryListKeys(AsciiBuffer map, AsciiBuffer first, int max) throws KeyNotFoundException {
            return null;
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

        public void transactionRemoveMessage(Buffer txid, AsciiBuffer queueName, Long messageKey) throws KeyNotFoundException {
        }

        public void transactionRollback(Buffer txid) throws KeyNotFoundException {
        }
    }

    public <R, T extends Exception> R execute(final Callback<R, T> callback, final Runnable onFlush) throws T {
        KahaDBSession session = new KahaDBSession();
        try {
            R rc = callback.execute(session);
            session.commit(onFlush);
            return rc;
        } finally {
            session.close();
        }
    }

    public void flush() {
        try {
            final CountDownLatch done = new CountDownLatch(1);
            synchronized (journal) {
                journal.write(FLUSH_DATA, new Runnable() {
                    public void run() {
                        done.countDown();
                    }
                });
            }

            // Keep trying waiting for the flush to happen unless the store
            // has been stopped.
            while (started.get()) {
                if (done.await(100, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (Exception e) {
            throw new FatalStoreException(e);
        }
    }

    // /////////////////////////////////////////////////////////////////
    // IoC Properties.
    // /////////////////////////////////////////////////////////////////

    protected PageFile createPageFile() {
        PageFile index = new PageFile(directory, "db");
        index.setEnableWriteThread(isEnableIndexWriteAsync());
        index.setWriteBatchSize(getIndexWriteBatchSize());
        return index;
    }

    protected Journal createJournal() {
        Journal manager = new Journal();
        manager.setDirectory(directory);
        manager.setMaxFileLength(getJournalMaxFileLength());
        return manager;
    }

    public File getDirectory() {
        return directory;
    }

    public void setStoreDirectory(File directory) {
        this.directory = directory;
    }

    public boolean isDeleteAllMessages() {
        return deleteAllMessages;
    }

    public void setDeleteAllMessages(boolean deleteAllMessages) {
        this.deleteAllMessages = deleteAllMessages;
    }

    public void setIndexWriteBatchSize(int setIndexWriteBatchSize) {
        this.setIndexWriteBatchSize = setIndexWriteBatchSize;
    }

    public int getIndexWriteBatchSize() {
        return setIndexWriteBatchSize;
    }

    public void setEnableIndexWriteAsync(boolean enableIndexWriteAsync) {
        this.enableIndexWriteAsync = enableIndexWriteAsync;
    }

    boolean isEnableIndexWriteAsync() {
        return enableIndexWriteAsync;
    }

    public boolean isEnableJournalDiskSyncs() {
        return enableJournalDiskSyncs;
    }

    public void setEnableJournalDiskSyncs(boolean syncWrites) {
        this.enableJournalDiskSyncs = syncWrites;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public long getCleanupInterval() {
        return cleanupInterval;
    }

    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    public void setJournalMaxFileLength(int journalMaxFileLength) {
        this.journalMaxFileLength = journalMaxFileLength;
    }

    public int getJournalMaxFileLength() {
        return journalMaxFileLength;
    }

    public PageFile getPageFile() {
        if (pageFile == null) {
            pageFile = createPageFile();
        }
        return pageFile;
    }

    public Journal getJournal() {
        if (journal == null) {
            journal = createJournal();
        }
        return journal;
    }

    public boolean isFailIfDatabaseIsLocked() {
        return failIfDatabaseIsLocked;
    }

    public void setFailIfDatabaseIsLocked(boolean failIfDatabaseIsLocked) {
        this.failIfDatabaseIsLocked = failIfDatabaseIsLocked;
    }

}
