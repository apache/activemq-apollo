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
package org.apache.activemq.broker.store.hawtdb.store;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.apollo.store.QueueRecord;
import org.apache.activemq.broker.store.hawtdb.model.*;
import org.apache.activemq.broker.store.hawtdb.model.Type.*;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.proto.InvalidProtocolBufferException;
import org.fusesource.hawtbuf.proto.MessageBuffer;
import org.fusesource.hawtbuf.proto.PBMessage;
import org.apache.activemq.util.LockFile;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fusesource.hawtdb.api.Transaction;
import org.fusesource.hawtdb.api.TxPageFile;
import org.fusesource.hawtdb.api.TxPageFileFactory;
import org.fusesource.hawtdb.internal.journal.Journal;
import org.fusesource.hawtdb.internal.journal.JournalCallback;
import org.fusesource.hawtdb.internal.journal.Location;

public class HawtDBManager {

    // Message related methods.

    static final int BEGIN_UNIT_OF_WORK = -1;
    static final int END_UNIT_OF_WORK = -2;
    static final int FLUSH = -3;
    static final int CANCEL_UNIT_OF_WORK = -4;

    static final Log LOG = LogFactory.getLog(HawtDBManager.class);
    static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    static final Buffer BEGIN_UNIT_OF_WORK_DATA = new Buffer(new byte[] { BEGIN_UNIT_OF_WORK });
    static final Buffer END_UNIT_OF_WORK_DATA = new Buffer(new byte[] { END_UNIT_OF_WORK });
    static final Buffer CANCEL_UNIT_OF_WORK_DATA = new Buffer(new byte[] { CANCEL_UNIT_OF_WORK });
    static final Buffer FLUSH_DATA = new Buffer(new byte[] { FLUSH });

    public static final int CLOSED_STATE = 1;
    public static final int OPEN_STATE = 2;

    protected TxPageFileFactory pageFileFactory = new TxPageFileFactory();
    public TxPageFile pageFile;
    protected Journal journal;

    protected RootEntity rootEntity = new RootEntity();

    protected boolean failIfDatabaseIsLocked;
    protected boolean deleteAllMessages;
    protected File directory;
    protected Thread checkpointThread;

    long checkpointInterval = 5 * 1000;
    long cleanupInterval = 30 * 1000;

    protected AtomicBoolean started = new AtomicBoolean();
    protected AtomicBoolean opened = new AtomicBoolean();
    private LockFile lockFile;
    private Location nextRecoveryPosition;
    private Location lastRecoveryPosition;
    private AtomicLong trackingGen = new AtomicLong(0);

    protected final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();
    private final HashSet<Integer> journalFilesBeingReplicated = new HashSet<Integer>();
    private boolean recovering;
    private int journalMaxFileLength = 1024*1024*20;

    private static class UoWOperation {
        public TypeCreatable bean;
        public Location location;
        public Buffer data;
    }

    // /////////////////////////////////////////////////////////////////
    // Lifecylce methods
    // /////////////////////////////////////////////////////////////////
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            try {
                load();

            } catch (Exception e) {
                LOG.error("Error loading store", e);
            }
        }
    }

    public void start(Runnable onComplete) throws Exception {
        start();
        if( onComplete!=null ) {
            onComplete.run();
        }
    }

    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            flush();
            unload();
        }
    }

    public void stop(Runnable onComplete) throws Exception {
        stop();
        if( onComplete!=null ) {
            onComplete.run();
        }
    }

    /**
     * @return a unique sequential store tracking number.
     */
    public long allocateStoreTracking() {
        return trackingGen.incrementAndGet();
    }

    public boolean isTransactional() {
        return true;
    }

    private void loadPageFile() throws IOException {
        indexLock.writeLock().lock();
        try {
            final TxPageFile pageFile = getPageFile();
            execute(new Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    if ( !tx.allocator().isAllocated(0) ) {
                        rootEntity.allocate(tx);
                    }
                    rootEntity.load(tx);
                }
            });
            pageFile.flush();

        } finally {
            indexLock.writeLock().unlock();
        }
    }

    interface Closure<T extends Exception> {
        public void execute(Transaction tx) throws T;
    }

    private <T extends Exception> void execute(Closure<T> closure) throws T {
        Transaction tx = pageFile.tx();
        boolean committed=false;
        try {
            closure.execute(tx);
            tx.commit();
            committed=true;
        } finally {
            if( !committed ) {
                tx.rollback();
            }
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

            if (deleteAllMessages) {
                getJournal().start();
                journal.delete();
                journal.close();
                journal = null;
                pageFileFactory.getFile().delete();
                rootEntity = new RootEntity();
                LOG.info("Persistence store purged.");
                deleteAllMessages = false;
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
            trackingGen.set(rootEntity.getLastMessageTracking() + 1);
        }
    }

    private void load() throws IOException {
        indexLock.writeLock().lock();
        try {
            open();

            store(new AddTrace.Bean().setMessage(new AsciiBuffer("LOADED " + new Date())), null);
        } finally {
            indexLock.writeLock().unlock();
        }

    }

    public void close() throws IOException, InterruptedException {
        if (opened.compareAndSet(true, false)) {

            indexLock.writeLock().lock();
            try {
                pageFileFactory.close();
                pageFile = null;
                rootEntity = new RootEntity();
                journal.close();
            } finally {
                indexLock.writeLock().unlock();
            }

            checkpointThread.join();
            lockFile.unlock();
            lockFile = null;
        }
    }

    public void unload() throws IOException, InterruptedException {
        if (pageFile !=null) {
            indexLock.writeLock().lock();
            try {
                rootEntity.setState(CLOSED_STATE);
                execute(new Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        // Set the last update to the next update (otherwise
                        // we'll replay the last update
                        // since location marshaller doesn't marshal the
                        // location's size:
                        rootEntity.setLastUpdate(journal.getNextLocation(rootEntity.getLastUpdate()));
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
     * @throws IllegalStateException
     */
    private void recover() throws IllegalStateException, IOException {
        indexLock.writeLock().lock();
        try {
            long start = System.currentTimeMillis();
            recovering = true;
            Location recoveryPosition = getRecoveryPosition();
            if (recoveryPosition != null) {
                int redoCounter = 0;
                Transaction uow = null;
                int uowCounter = 0;
                while (recoveryPosition != null) {

                    Buffer data = journal.read(recoveryPosition);
                    if (data.length == 1 && data.data[0] == BEGIN_UNIT_OF_WORK) {
                        uow = pageFile.tx();
                    } else if (data.length == 1 && data.data[0] == END_UNIT_OF_WORK) {
                        if (uow != null) {
                            rootEntity.setLastUpdate(recoveryPosition);
                            uow.commit();
                            redoCounter += uowCounter;
                            uowCounter = 0;
                            uow = null;
                        }
                    } else if (data.length == 1 && data.data[0] == CANCEL_UNIT_OF_WORK) {
                        uow.rollback();
                        uow = null;
                    } else if (data.length == 1 && data.data[0] == FLUSH) {
                    } else {
                        final TypeCreatable message = load(recoveryPosition);
                        final Location location = recoveryPosition;
                        if (uow != null) {
                            updateIndex(uow, message.toType(), (MessageBuffer) message, location);
                            uowCounter++;
                        } else {
                            execute(new Closure<IOException>() {
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
            execute(new Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    recoverIndex(tx);
                }
            });
        } finally {
            recovering = false;
            indexLock.writeLock().unlock();
        }
    }

    public void incrementalRecover() throws IOException {
        indexLock.writeLock().lock();
        try {
            recovering = true;
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

                execute(new Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, message.toType(), (MessageBuffer) message, location);
                    }
                });

                nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
            }
        } finally {
            recovering = false;
            indexLock.writeLock().unlock();
        }
    }

    protected void recoverIndex(Transaction tx) throws IOException {
        long start = System.currentTimeMillis();
        // It is possible index updates got applied before the journal updates..
        // in that case we need to removed references to messages that are not
        // in the journal
        final Location lastAppendLocation = journal.getLastAppendLocation();
        int undoCounter = rootEntity.recoverIndex(lastAppendLocation, tx);

        long end = System.currentTimeMillis();
        if (undoCounter > 0) {
            // The rolledback operations are basically in flight journal writes.
            // To avoid getting these the end user
            // should do sync writes to the journal.
            LOG.info("Rolled back " + undoCounter + " operations from the index in " + ((end - start) / 1000.0f) + " seconds.");
        }
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
            indexLock.writeLock().lock();
            long start = System.currentTimeMillis();

            try {
                if (!opened.get()) {
                    return;
                }
                execute(new Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        checkpointUpdate(tx, cleanup);
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
            long end = System.currentTimeMillis();
            if (end - start > 1000) {
                if (cleanup) {
                    LOG.warn("KahaDB Cleanup took " + (end - start));
                } else {
                    LOG.warn("KahaDB CheckPoint took " + (end - start));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void checkpoint(org.apache.activemq.util.Callback closure) throws Exception {
        indexLock.writeLock().lock();
        try {
            execute(new Closure<IOException>() {
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

        if (LOG.isErrorEnabled()) {
            LOG.debug("Checkpoint started.");
        }

        rootEntity.setState(OPEN_STATE);
        // Set the last update to the next update (otherwise we'll replay the
        // last update
        // since location marshaller doesn't marshal the location's size:
        rootEntity.setLastUpdate(journal.getNextLocation(rootEntity.getLastUpdate()));
        rootEntity.store(tx);
        pageFile.flush();

        if (cleanup) {

            final TreeSet<Integer> gcCandidateSet = new TreeSet<Integer>(journal.getFileMap().keySet());

            // Don't GC files under replication
            if (journalFilesBeingReplicated != null) {
                gcCandidateSet.removeAll(journalFilesBeingReplicated);
            }

            rootEntity.removeGCCandidates(gcCandidateSet, tx);

            if (!gcCandidateSet.isEmpty()) {
                if (LOG.isErrorEnabled()) {
                    LOG.debug("Cleanup removing the data files: " + gcCandidateSet);
                }
                journal.removeDataFiles(gcCandidateSet);
            }
        }

        if (LOG.isErrorEnabled()) {
            LOG.debug("Checkpoint done.");
        }
    }

    public HashSet<Integer> getJournalFilesBeingReplicated() {
        return journalFilesBeingReplicated;
    }

    // /////////////////////////////////////////////////////////////////
    // Store interface
    // /////////////////////////////////////////////////////////////////
    long messageSequence;

    private Location store(TypeCreatable data, Transaction tx) throws IOException {
        return store(data, null, tx);
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
    public Location store(final TypeCreatable data, final Runnable onFlush, Transaction tx) throws IOException {
        final MessageBuffer message = ((PBMessage) data).freeze();
        int size = message.serializedSizeUnframed();
        DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
        os.writeByte(data.toType().getNumber());
        message.writeUnframed(os);

        //If we aren't in a transaction acquire the index lock
        if (tx == null) {
            indexLock.writeLock().lock();
        }

        try {

            long start = System.currentTimeMillis();
            final Location location;
            synchronized (journal) {
                location = journal.write(os.toBuffer(), new JournalCallback(){
                    public void success(Location location) {
                        if( onFlush!=null ) {
                            onFlush.run();
                        }
                    }
                });
            }
            long start2 = System.currentTimeMillis();

            if (tx == null) {
                execute(new Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, data.toType(), message, location);
                    }
                });
            } else {
                updateIndex(tx, data.toType(), message, location);
            }

            long end = System.currentTimeMillis();
            if (end - start > 1000) {
                LOG.warn("KahaDB long enqueue time: Journal Add Took: " + (start2 - start) + " ms, Index Update took " + (end - start2) + " ms");
            }
            return location;

        } finally {
            if (tx == null)
                indexLock.writeLock().unlock();
        }

    }

    /**
     * Loads a previously stored PBMessage
     * 
     * @param location
     * @return
     * @throws IOException
     */
    TypeCreatable load(Location location) throws IOException {
        Buffer data = journal.read(location);
        return load(location, data);
    }

    private TypeCreatable load(Location location, Buffer data) throws IOException, InvalidProtocolBufferException {
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
        // System.out.println("Updating index" + type.toString() + " loc: " +
        // location);
        switch (type) {
        case ADD_MESSAGE:
            messageAdd(tx, (AddMessage.Getter) command, location);
            break;
        case ADD_QUEUE:
            queueAdd(tx, (AddQueue.Getter) command, location);
            break;
        case REMOVE_QUEUE:
            queueRemove(tx, (RemoveQueue.Getter) command, location);
            break;
//        case QUEUE_ADD_ENTRY:
//            queueAddMessage(tx, (AddQueueEntry) command, location);
//            break;
//        case QUEUE_REMOVE_ENTRY:
//            queueRemoveMessage(tx, (RemoveQueueEntry) command, location);
//            break;
//        case SUBSCRIPTION_ADD:
//            rootEntity.addSubscription((AddSubscription) command);
//            break;
//        case SUBSCRIPTION_REMOVE:
//            rootEntity.removeSubscription(((RemoveSubscription) command).getName());
//            break;
//        case TRANSACTION_BEGIN:
//        case TRANSACTION_ADD_MESSAGE:
//        case TRANSACTION_REMOVE_MESSAGE:
//        case TRANSACTION_COMMIT:
//        case TRANSACTION_ROLLBACK:
//        case MAP_ADD:
//            rootEntity.mapAdd(((AddMap) command).getMapName(), tx);
//            break;
//        case MAP_REMOVE:
//            rootEntity.mapRemove(((RemoveMap) command).getMapName(), tx);
//            break;
//        case MAP_ENTRY_PUT: {
//            PutMapEntry p = (PutMapEntry) command;
//            rootEntity.mapAddEntry(p.getMapName(), p.getId(), p.getValue(), tx);
//            break;
//        }
//        case MAP_ENTRY_REMOVE: {
//            RemoveMapEntry p = (RemoveMapEntry) command;
//            try {
//                rootEntity.mapRemoveEntry(p.getMapName(), p.getId(), tx);
//            } catch (KeyNotFoundException e) {
//                //yay, removed.
//            }
//            break;
//        }
//        case STREAM_OPEN:
//        case STREAM_WRITE:
//        case STREAM_CLOSE:
//        case STREAM_REMOVE:
//            throw new UnsupportedOperationException();
        }
        rootEntity.setLastUpdate(location);
    }

    private void messageAdd(Transaction tx, AddMessage.Getter command, Location location) throws IOException {
        rootEntity.messageAdd(tx, command, location);
    }

    private void queueAdd(Transaction tx, AddQueue.Getter command, Location location) throws IOException {
        QueueRecord qd = new QueueRecord();
        qd.name = command.getName();
        qd.queueType = command.getQueueType();
//        if (command.hasParentName()) {
//            qd.setParent(command.getParentName());
//            qd.setPartitionId(command.getPartitionId());
//        }

        rootEntity.queueAdd(tx, qd);
    }

    private void queueRemove(Transaction tx, RemoveQueue.Getter command, Location location) throws IOException {
        rootEntity.queueRemove(tx, command.getKey());
    }

    private void queueAddMessage(Transaction tx, AddQueueEntry.Getter command, Location location) throws IOException {
        QueueRecord qd = new QueueRecord();
        DestinationEntity destination = rootEntity.getDestination(command.getQueueKey());
        if (destination != null) {
            try {
                destination.add(tx, command);
            } catch (DuplicateKeyException e) {
                if (!recovering) {
                    throw new FatalStoreException(e);
                }
            }
            rootEntity.addMessageRef(tx, command.getMessageKey());
        }
    }

    private void queueRemoveMessage(Transaction tx, RemoveQueueEntry.Getter command, Location location) throws IOException {
        DestinationEntity destination = rootEntity.getDestination(command.getQueueKey());
        if (destination != null) {
            long messageKey = destination.remove(tx, command.getQueueKey());
            if (messageKey >= 0) {
                rootEntity.removeMessageRef(tx, command.getQueueKey());
            }
        }
    }

    public HawtDBSession getSession() {
        return new HawtDBSession(this);
    }

    /**
     * Convenienct method for executing a batch of work within a store
     * transaction.
     */
    public <R, T extends Exception> R execute(final Callback<R, T> callback, final Runnable onFlush) throws T {
        HawtDBSession session = new HawtDBSession(this);
        session.acquireLock();
        try {
            R rc = callback.execute(session);
            session.commit(onFlush);
            return rc;
        } finally {
            session.releaseLock();
        }
    }

    public void flush() {
        try {
            final CountDownLatch done = new CountDownLatch(1);
            synchronized (journal) {
                journal.write(FLUSH_DATA, new JournalCallback(){
                    public void success(Location location) {
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

    private TxPageFile getPageFile() {
        if (pageFile == null) {
            pageFileFactory.setFile(new File(directory, "db"));
            pageFileFactory.setDrainOnClose(false);
            pageFileFactory.setSync(true);
            pageFileFactory.setUseWorkerThread(true);
            pageFileFactory.open();
            pageFile = pageFileFactory.getTxPageFile();
        }
        return pageFile;
    }

    private Journal getJournal() {
        if (journal == null) {
            journal = new Journal();
            journal.setDirectory(directory);
            journal.setMaxFileLength(getJournalMaxFileLength());
        }
        return journal;
    }

    public File getDirectory() {
        return directory;
    }

    public File getStoreDirectory() {
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

    public boolean isFailIfDatabaseIsLocked() {
        return failIfDatabaseIsLocked;
    }

    public void setFailIfDatabaseIsLocked(boolean failIfDatabaseIsLocked) {
        this.failIfDatabaseIsLocked = failIfDatabaseIsLocked;
    }

    public int getJournalMaxFileLength() {
        return journalMaxFileLength;
    }
    public void setJournalMaxFileLength(int journalMaxFileLength) {
        this.journalMaxFileLength = journalMaxFileLength;
    }

    public int getIndexMaxPages() {
        return pageFileFactory.getMaxPages();
    }
    public void setIndexMaxPages(int maxPages) {
        pageFileFactory.setMaxPages(maxPages);
    }

    public short getIndexPageSize() {
        return pageFileFactory.getPageSize();
    }
    public void setIndexPageSize(short pageSize) {
        pageFileFactory.setPageSize(pageSize);
    }

    public int getIndexMappingSegementSize() {
        return pageFileFactory.getMappingSegementSize();
    }
    public void setIndexMappingSegementSize(int mappingSegementSize) {
        pageFileFactory.setMappingSegementSize(mappingSegementSize);
    }
}
