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
package org.apache.hawtdb.internal.page;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.zip.CRC32;

import javolution.io.Struct;

import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.list.LinkedNodeList;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.PagingException;
import org.apache.hawtdb.api.Transaction;
import org.apache.hawtdb.api.Paged.SliceType;
import org.apache.hawtdb.internal.io.MemoryMappedFile;
import org.apache.hawtdb.internal.util.Ranges;


/**
 * Provides concurrent page file access via Multiversion concurrency control
 * (MVCC).
 * 
 * Once a transaction begins working against the data, it acquires a snapshot of
 * all the data in the page file. This snapshot is used to provides the
 * transaction consistent view of the data in spite of it being concurrently
 * modified by other transactions.
 * 
 * When a transaction does a page update, the update is stored in a temporary
 * page location. Subsequent reads of the original page will result in page read
 * of the temporary page. If the transaction rolls back, the temporary pages are
 * freed. If the transaction commits, the page updates are assigned the next
 * snapshot version number and the update gets queued so that it can be applied
 * atomically at a later time.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class HawtPageFile {

    public static final int FILE_HEADER_SIZE = 1024 * 4;
    public static final String MAGIC = "HawtDB:1.0\n";

    /**
     * The first 4K of the file is used to hold 2 copies of the header.
     * Each copy is 2K big.  The header is checksummed so that corruption
     * can be detected. 
     */
    static private class Header extends Struct {
        
        /** Identifies the file format */
        public final UTF8String magic = new UTF8String(32);
        /** The oldest applied commit revision */
        public final Signed64 base_revision = new Signed64();
        /** The size of each page in the page file */
        public final Signed32 page_size = new Signed32();
        /** The page location of the free page list */
        public final Signed32 free_list_page = new Signed32();
        /** points at the latest batch page which is guaranteed to be fully stored */
        public final Signed32 stored_batch_page = new Signed32();
        /** The page location of the latest batch page. Not guaranteed to be fully stored */ 
        public final Signed32 storing_batch_page = new Signed32();
        
        /** The size of all the previous fields */
        private static final int USED_FIELDS_SIZE = 32 + 8 + 4 + 4 + 4 + 4;

        /** reserves header space for future use */
        public final UTF8String reserved = new UTF8String((FILE_HEADER_SIZE/2)-(USED_FIELDS_SIZE+8));
        
        /** a checksum of all the previous fields. The reserved space 
         * positions this right at the end of a 2k block */
        public final Signed64 checksum = new Signed64();
        
        public String toString() { 
            return "{ base_revision: "+this.base_revision.get()+
            ", page_size: "+page_size.get()+", free_list_page: "+free_list_page.get()+
            ", storing batch: "+storing_batch_page.get()+", checksum: "+checksum.get()+
            " }";
        }
    }
    /** The header structure of the file */
    private final Header header = new Header();
    private final LinkedNodeList<Batch> batches = new LinkedNodeList<Batch>();

    private final MemoryMappedFile file;
    final SimpleAllocator allocator;
    final PageFile pageFile;
    private static final int updateBatchSize = 1024;
    private final boolean synch;
    private int lastBatchPage = -1;
    
    //
    // The following batch objects point to linked nodes in the previous batch list.  
    // They are used to track/designate the state of the batch object.
    //
    
    /** The current batch that is currently being assembled. */
    Batch openBatch;
    /** The batches that are being stored... These might be be recoverable. */
    Batch storingBatches;
    /** The stored batches. */
    Batch storedBatches;
    /** The performed batches.  Page updates have been copied from the redo pages to the original page locations. */
    Batch performedBatches;
    
    /** Used as read cache */
    ReadCache readCache = new ReadCache();

    //
    // Profilers like yourkit just tell which mutex class was locked.. so create a different class for each mutex
    // so we can more easily tell which mutex was locked.
    //
    private static class HOUSE_KEEPING_MUTEX { public String toString() { return "HOUSE_KEEPING_MUTEX"; }}
    private static class TRANSACTION_MUTEX { public String toString() { return "TRANSACTION_MUTEX"; }}

    /** 
     * Mutex for data structures which are used during house keeping tasks like batch
     * management. Once acquired, you can also acquire the TRANSACTION_MUTEX 
     */
    private final HOUSE_KEEPING_MUTEX HOUSE_KEEPING_MUTEX = new HOUSE_KEEPING_MUTEX();

    /** 
     * Mutex for data structures which transaction threads access. Never attempt to 
     * acquire the HOUSE_KEEPING_MUTEX once this mutex is acquired.  
     */
    final TRANSACTION_MUTEX TRANSACTION_MUTEX = new TRANSACTION_MUTEX();
    
    /**
     * This is the free page list at the base revision.  It does not 
     * track allocations in transactions or committed updates.  Only 
     * when the updates are performed will this list be updated.
     * 
     * The main purpose of this list is to initialize the free list 
     * on recovery.
     * 
     * This does not track the space associated with batch lists 
     * and free lists.  On recovery that space is discovered and 
     * tracked in the page file allocator.
     */
    private Ranges storedFreeList = new Ranges();
    private final ExecutorService worker;
    
    public HawtPageFile(HawtPageFileFactory factory) {
        this.pageFile = factory.getPageFile();
        this.synch = factory.isSync();
        this.file = pageFile.getFile();
        this.allocator = pageFile.allocator();
        ByteBuffer slice = file.slice(false, 0, FILE_HEADER_SIZE);
        this.header.setByteBuffer(slice, slice.position());
        
        if( factory.isUseWorkerThread() ) {
            worker = Executors.newSingleThreadExecutor(new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread rc = new Thread(r);
                    rc.setName("HawtDB Worker");
                    rc.setDaemon(true);
                    rc.start();
                    return rc;
                }
            });
        } else {
            worker = null;
        }
    }
    
    @Override
    public String toString() {
        return "{\n" +
    		"  allocator: "+allocator+ ",\n"+
    		"  synch: "+synch+ ",\n"+
    		"  read cache size: "+readCache.map.size()+ ",\n"+
    		"  base revision free pages: "+storedFreeList + ",\n"+
    		"  batches: {\n"+ 
    		"    performed: "+toString(performedBatches, storedBatches) + ",\n"+ 
    		"    stored: "+toString(storedBatches, storingBatches) + ",\n"+
    		"    storing: "+toString(storingBatches, openBatch)+ ",\n"+
    		"    open: "+toString(openBatch, null)+ ",\n"+
    		"  }"+ "\n"+
    		"}";
    }

    /** 
     * @param from
     * @param to
     * @return string representation of the batch items from the specified batch up to (exclusive) the specified batch.
     */
    private String toString(Batch from, Batch to) {
        StringBuilder rc = new StringBuilder();
        rc.append("[ ");
        Batch t = from;
        while( t!=null && t!=to ) {
            if( t!=from ) {
                rc.append(", ");
            }
            rc.append(t);
            t = t.getNext();
        }
        rc.append(" ]");
        return rc.toString();
    }

    public Transaction tx() {
        return new HawtTransaction(this);
    }

    /**
     * Attempts to commit a set of page updates.
     * 
     * @param snapshot
     * @param pageUpdates
     * @param deferredUpdates
     */
    void commit(Snapshot snapshot, ConcurrentHashMap<Integer, Update> pageUpdates) {
        
        boolean fullBatch=false;
        Commit commit=null;
        synchronized (TRANSACTION_MUTEX) {
            
            // we need to figure out the revision id of the this commit...
            long rev;
            if( snapshot!=null ) {
                
                // Lets check for an OptimisticUpdateException
                // verify that the new commit's updates don't conflict with a commit that occurred
                // subsequent to the snapshot that this commit started operating on.
                
                // Note: every deferred update has an entry in the pageUpdates, so no need to 
                // check to see if that map also conflicts.
                rev = snapshot.getHead().commitCheck(pageUpdates);
                snapshot.close();
            } else {
                rev = openBatch.head;
            }
            rev++;

            BatchEntry last = openBatch.entries.getTail();
            if( last!=null ) {
                commit = last.isCommit();
            }
            
            if( commit!=null ) {
                // TODO: figure out how to do the merge outside the TRANSACTION_MUTEX
                commit.merge(pageFile.allocator(), rev, pageUpdates);
            } else {
                commit = new Commit(rev, pageUpdates);
                openBatch.entries.addLast(commit);
            }
            
            if( openBatch.base == -1 ) {
                openBatch.base = rev;
            }
            openBatch.head = rev;

            
            if( openBatch.pageCount() > updateBatchSize ) {
                fullBatch = true;
            }
        }
        
        if( fullBatch ) {
            synchronized (HOUSE_KEEPING_MUTEX) {
                storeBatches(false);
            }
            if( worker!=null ) {
                worker.execute(new Runnable() {
                    public void run() {
                        flushBatch();
                    }
                });
            } else {
                flushBatch();
            }
        }
    }

    private void flushBatch() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            // TODO: do the following actions async.
            syncBatches();
            performBatches();
        }
    }
    
    /**
     * Used to initialize a new file or to clear out the 
     * contents of an existing file.
     */
    public void reset() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            batches.clear();
            performedBatches = storedBatches = storingBatches = openBatch = new Batch(-1);
            batches.addFirst(openBatch);
            
            lastBatchPage = -1;
            readCache.clear();
            
            allocator.clear(); 
            storedFreeList.clear();
            storedFreeList.add(0, allocator.getLimit());
    
            // Initialize the file header..
            Header h = header();
            h.setByteBufferPosition(0);
            h.magic.set(MAGIC);
            h.base_revision.set(-1);
            h.free_list_page.set(-1);
            h.page_size.set(pageFile.getPageSize());
            h.reserved.set("");
            h.storing_batch_page.set(-1);
            replicateHeader();
        }
    }    
    /**
     * Loads an existing file and replays the batch
     * logs to put it in a consistent state.
     */
    public void recover() {
        synchronized (HOUSE_KEEPING_MUTEX) {

            batches.clear();
            performedBatches = storedBatches = storingBatches = openBatch = new Batch(-1);
            batches.addFirst(openBatch);
            lastBatchPage = -1;
            readCache.clear();
    
            Header h = header();
            if( !MAGIC.equals( h.magic.get()) ) {
                throw new PagingException("The file header is not of the expected type.");
            }
            
            long baseRevision = h.base_revision.get();
    
            // Initialize the free page list.
            int pageId = h.free_list_page.get();
            if( pageId >= 0 ) {
                storedFreeList = loadObject(pageId);
                allocator.copy(storedFreeList);
                Extent.unfree(pageFile, pageId);
            } else {
                allocator.clear(); 
                storedFreeList.add(0, allocator.getLimit());
            }
            
            boolean consistencyCheckNeeded=true;
            int last_synced_batch = h.stored_batch_page.get();
            pageId = h.storing_batch_page.get();
            if( pageId<0 ) {
                pageId = last_synced_batch;
                consistencyCheckNeeded = false;
            }
            while( true ) {
                if( pageId < 0 ) {
                    break;
                }

                if( consistencyCheckNeeded ) {
                    // TODO: when consistencyCheckNeeded==true, then we need to check the
                    // Consistency of the batch, as it may have been partially written to disk.
                }
                
                
                Batch batch = loadObject(pageId); 
                batch.page = pageId;
                batch.recovered = true;
                Extent.unfree(pageFile, pageId);
                
                if( openBatch.head == -1 ) {
                    openBatch.head = batch.head;
                }
    
                if( baseRevision < batch.head ) {
                    // add first since we are loading batch objects oldest to youngest
                    // but want to put them in the list youngest to oldest.
                    batches.addFirst(batch);
                    performedBatches = storedBatches = batch;
                    pageId=batch.previous;
                    if( pageId==last_synced_batch ) {
                        consistencyCheckNeeded = false;
                    }
                } else {
                    break;
                }
            }
            
            // Apply all the batches..
            performBatches();
        }        
    }

    /**
     * Once this method returns, any previously committed transactions 
     * are flushed and to the disk, ensuring that they will not be lost
     * upon failure.
     */
    public void flush() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            storeBatches(true);
            syncBatches();
        }
    }   
    
    // /////////////////////////////////////////////////////////////////
    //
    // Methods which transition bathes through their life cycle states;
    //
    //    open -> storing -> stored -> performed -> released
    //
    // The HOUSE_KEEPING_MUTEX must be acquired before being called. 
    //
    // /////////////////////////////////////////////////////////////////
    
    /**
     * Attempts to perform a batch state change: open -> storing
     */
    private void storeBatches(boolean force) {
        Batch batch;
        
        // We synchronized /w the transactions so that they see the state change.
        synchronized (TRANSACTION_MUTEX) {
            // Re-checking since storing the batch may not be needed.
            if( (force && openBatch.base!=-1 ) || openBatch.pageCount() > updateBatchSize ) {
                batch = openBatch;
                openBatch = new Batch(batch.head);
                batches.addLast(openBatch);
            } else {
                return;
            }
        }
        
        // Write any outstanding deferred cache updates...
        batch.performDefferedUpdates(pageFile);

        // Link it to the last batch.
        batch.previous = lastBatchPage; 
        
        // Store the batch record.
        lastBatchPage = batch.page = storeObject(batch);

        // Update the header to know about the new batch page.
        header().storing_batch_page.set(batch.page);
        replicateHeader();
    }
    
    /**
     * Performs a file sync. 
     * 
     * This allows two types of batch state changes to occur:
     * <ul>
     * <li> storing -> stored
     * <li> performed -> released
     * </ul>
     */
    private void syncBatches() {

        // This is a slow operation..
        if( synch ) {
            file.sync();
        }
        Header h = header();

        // Update the base_revision with the last performed revision.
        if (performedBatches!=storedBatches) {
            Batch lastPerformedBatch = storedBatches.getPrevious();
            h.base_revision.set(lastPerformedBatch.head);
        }

        // Were there some batches in the stored state?
        if (storingBatches!=openBatch) {
            
            // The last stored is actually synced now..
            Batch lastStoredBatch = openBatch.getPrevious();
            // Let the header know about it..
            h.stored_batch_page.set(lastStoredBatch.page);
            
            // We synchronized /w the transactions so that they see the state change.
            synchronized (TRANSACTION_MUTEX) {
                // Transition stored -> synced.
                storingBatches = openBatch;
            }
        }
        
        // Once a batch has been performed, subsequently synced, and no longer referenced,
        // it's allocated recovery space can be released.
        while( performedBatches!=storedBatches ) {
            if( performedBatches.snapshots!=0 ) {
                break;
            }
            
            // Free the update pages associated with the batch.
            performedBatches.release(allocator);
            
            // Free the batch record itself.
            Extent.free(pageFile, performedBatches.page);
            
            // don't need to sync /w transactions since they don't use the performedBatches variable.
            // Transition performed -> released
            performedBatches = performedBatches.getNext();
            
            // removes the released batch form the batch list.
            performedBatches.getPrevious().unlink();
        }

        // Store the free list..
        int previousFreeListPage = h.free_list_page.get();
        h.free_list_page.set(storeObject(storedFreeList));
        replicateHeader();

        // Release the previous free list.
        if (previousFreeListPage >= 0) {
            Extent.free(pageFile, previousFreeListPage);
        }
    }

    /**
     * Attempts to perform a batch state change: stored -> performed
     * 
     * Once a batch is performed, new snapshots will not reference 
     * the batch anymore.
     */
    public void performBatches() {

        if( storedBatches==storingBatches ) {
            // There are no batches in the synced state for use to transition.
            return;
        }
              
        // The last performed batch MIGHT still have an open snapshot.
        // we can't transition from synced, until that snapshot closes.
        Batch lastPerformed = storedBatches.getPrevious();
        if( lastPerformed!=null && lastPerformed.snapshots!=0) {
            return;
        }
        
        while( storedBatches!=storingBatches ) {
            
            // Performing the batch actually applies the updates to the original page locations.
            for (Commit commit : storedBatches) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                    int page = entry.getKey();
                    Update update = entry.getValue();
                    
                    if( page != update.page ) {
                        
                        if( storedBatches.recovered ) {
                            // If we are recovering, the allocator MIGHT not have this 
                            // page as being allocated.  This makes sure it's allocated so that
                            // new transaction to get this page and overwrite it in error.
                            allocator.unfree(page, 1);
                        }
                        
                        // Perform the update by copying the updated page the original
                        // page location.
                        ByteBuffer slice = pageFile.slice(SliceType.READ, update.page, 1);
                        try {
                            pageFile.write(page, slice);
                        } finally { 
                            pageFile.unslice(slice);
                        }
                        
                    }
                    if( update.wasAllocated() ) {
                        
                        if( storedBatches.recovered ) {
                            // If we are recovering, the allocator MIGHT not have this 
                            // page as being allocated.  This makes sure it's allocated so that
                            // new transaction to get this page and overwrite it in error.
                            allocator.unfree(page, 1);
                        }
                        // Update the persistent free list.  This gets stored on the next sync.
                        storedFreeList.remove(page, 1);
                        
                    } else if( update.wasFreed() ) {
                        storedFreeList.add(page, 1);
                    }
                }
            }
            
            storedBatches.performed = true;
            
            // We synchronized /w the transactions so that they see the state change.
            synchronized (TRANSACTION_MUTEX) {
                // Transition synced -> performed
                storedBatches = storedBatches.getNext();
            }
            
            lastPerformed = storedBatches.getPrevious();
            // We have to stop if the last batch performed has an open snapshot.
            if( lastPerformed.snapshots!=0 ) {
                break;
            }
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // Snapshot management
    // /////////////////////////////////////////////////////////////////
    
    Snapshot openSnapshot() {
        synchronized(TRANSACTION_MUTEX) {
            SnapshotHead head=null;

            // re-use the last entry if it was a snapshot head..
            BatchEntry entry = openBatch.entries.getTail();
            if( entry!=null ) {
                head = entry.isSnapshotHead();
            }
            
            if( head == null ) {
                // create a new snapshot head entry..
                head = new SnapshotHead(openBatch);
                openBatch.entries.addLast(head);
            }
            
            // Open the snapshot off that head position.
            return new Snapshot(this, head, storedBatches).open();
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // TODO:
    // /////////////////////////////////////////////////////////////////
    
    /**
     * The quiesce method is used to pause/stop access to the concurrent page file.
     * access can be restored using the {@link #resume()} method.    
     * 
     * @param reads if true, the suspend will also suspend read only transactions. 
     * @param blocking if true, transactions will block until the {@link #resume()} method 
     *          is called, otherwise they will receive errors.
     * @param drain if true, in progress transactions are allowed to complete, otherwise they
     *        also are suspended. 
     */
    public void suspend(boolean reads, boolean blocking, boolean drain) {
    }

    /**
     * Resumes a previously suspended page file. 
     */
    public void resume() {
    }
    
    
    // /////////////////////////////////////////////////////////////////
    // Helper methods
    // /////////////////////////////////////////////////////////////////
    
    private int storeObject(Object value) {
        try {
            ExtentOutputStream eos = new ExtentOutputStream(pageFile);
            ObjectOutputStream oos = new ObjectOutputStream(eos);
            oos.writeObject(value);
            oos.close();
            return eos.getPage();
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T loadObject( int pageId ) {
        try {
            ExtentInputStream eis = new ExtentInputStream(pageFile, pageId);
            ObjectInputStream ois = new ObjectInputStream(eis);
            return (T) ois.readObject();
        } catch (IOException e) {
            throw new IOPagingException(e);
        } catch (ClassNotFoundException e) {
            throw new IOPagingException(e);
        }
    }
    

    private Header header() {
        this.header.getByteBuffer().position(0);
        this.header.setByteBufferPosition(0);
        Header h = this.header;
        return h;
    }
    
    private void replicateHeader() {
        // Calculate the checksum of the header so that we can tell if the
        // header is corrupted.
        byte[] data = new byte[this.header.size() - 8];
        file.read(0, data);
        CRC32 checksum = new CRC32();
        checksum.update(data);
        this.header.checksum.set(checksum.getValue());

        // Copy the header so we can survive a partial update.
        ByteBuffer header = file.read(0, this.header.size());
        file.write(FILE_HEADER_SIZE / 2, header);
    }    
    
    // /////////////////////////////////////////////////////////////////
    // Simple Helper Classes
    // /////////////////////////////////////////////////////////////////

    final class ReadCache {
        private final Map<Integer, Object> map = Collections.synchronizedMap(new LRUCache<Integer, Object>(1024));

        @SuppressWarnings("unchecked") <T> T cacheLoad(EncoderDecoder<T> marshaller, int pageId) {
            T rc = (T) map.get(pageId);
            if( rc ==null ) {
                rc = marshaller.load(pageFile, pageId);
                map.put(pageId, rc);
            }
            return rc;
        }

        public void clear() {
            map.clear();
        }        
    }    
}
