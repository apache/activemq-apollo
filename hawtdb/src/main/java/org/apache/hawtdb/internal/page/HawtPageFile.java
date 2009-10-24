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
        /** points at the latest redo page which is guaranteed to be fully stored */
        public final Signed32 redo_page = new Signed32();
        /** The page location of the latest redo page. Not guaranteed to be fully stored */ 
        public final Signed32 unsynced_redo_page = new Signed32();
        
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
            ", redo_page: "+unsynced_redo_page.get()+", checksum: "+checksum.get()+
            " }";
        }
    }

    private final MemoryMappedFile file;
    final SimpleAllocator allocator;
    final PageFile pageFile;
    private static final int updateBatchSize = 1024;
    private final boolean synch;

    /** The header structure of the file */
    private final Header header = new Header();
    
    int lastRedoPage = -1;
    
    private final LinkedNodeList<Batch> redos = new LinkedNodeList<Batch>();
    
    //
    // The following Redo objects point to linked nodes in the previous redo list.  
    // They are used to track designate the state of the redo object.
    //
    
    /** The current redo that is currently being built */
    Batch buildingRedo;
    /** The stored redos.  These might be be recoverable. */
    Batch storedRedos;
    /** The synced redos.  A file sync occurred after these redos were stored. */
    Batch syncedRedos;
    /** The performed redos.  Updates are actually performed to the original page file. */
    Batch performedRedos;
    
    /** Used as read cache */
    ReadCache readCache = new ReadCache();

    /** Mutex for data structures which are used during house keeping tasks like redo management. Once acquired, you can also acquire the TRANSACTION_MUTEX */
    private final Object HOUSE_KEEPING_MUTEX = "HOUSE_KEEPING_MUTEX";

    /** Mutex for data structures which transaction threads access. Never attempt to acquire the HOUSE_KEEPING_MUTEX once this mutex is acquired.  */
    final Object TRANSACTION_MUTEX = "TRANSACTION_MUTEX";
    

    /**
     * This is the free page list at the base revision.  It does not track allocations in transactions
     * or committed updates.  Only when the updates are squashed will this list be updated.
     * 
     * The main purpose of this list is to initialize the free list on recovery.
     * 
     * This does not track the space associated with redo batches and free lists.  On 
     * recovery that space is discovered and tracked in the allocator.
     */
    private Ranges baseRevisionFreePages = new Ranges();
    
    public HawtPageFile(HawtPageFileFactory factory) {
        this.pageFile = factory.getPageFile();
        this.synch = factory.isSync();
        this.file = pageFile.getFile();
        this.allocator = pageFile.allocator();
        ByteBuffer slice = file.slice(false, 0, FILE_HEADER_SIZE);
        this.header.setByteBuffer(slice, slice.position());
    }
    
    @Override
    public String toString() {
        return "{\n" +
        		"  allocator: "+allocator+ ",\n"+
        		"  synch: "+synch+ ",\n"+
        		"  read cache size: "+readCache.map.size()+ ",\n"+
        		"  base revision free pages: "+baseRevisionFreePages + ",\n"+
        		"  redos: {\n"+ 
        		"    performed: "+toString(performedRedos, syncedRedos) + ",\n"+ 
        		"    synced: "+toString(syncedRedos, storedRedos) + ",\n"+
        		"    stored: "+toString(storedRedos, buildingRedo)+ ",\n"+
        		"    building: "+toString(buildingRedo, null)+ ",\n"+
        		"  }"+ "\n"+
        		"}";
    }

    /** 
     * @param from
     * @param to
     * @return string representation of the redo items from the specified redo up to (exclusive) the specified redo.
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
        
        boolean fullRedo=false;
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
                rev = buildingRedo.head;
            }
            rev++;

            if( buildingRedo.base == -1 ) {
                buildingRedo.base = rev;
            }
            buildingRedo.head = rev;
            
            Commit commit=null;
            BatchEntry last = buildingRedo.entries.getTail();
            if( last!=null ) {
                commit = last.isCommit();
            }
            
            if( commit!=null ) {
                // TODO: figure out how to do the merge outside the TRANSACTION_MUTEX
                commit.merge(pageFile.allocator(), rev, pageUpdates);
            } else {
                buildingRedo.entries.addLast(new Commit(rev, pageUpdates) );
            }
            
            if( buildingRedo.pageCount() > updateBatchSize ) {
                fullRedo = true;
            }
        }
        
        if( fullRedo ) {
            synchronized (HOUSE_KEEPING_MUTEX) {
                storeRedos(false);
                // TODO: do the following actions async.
                syncRedos();
                performRedos();
            }
        }
    }
    
    /**
     * Used to initialize a new file or to clear out the 
     * contents of an existing file.
     */
    public void reset() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            redos.clear();
            performedRedos = syncedRedos = storedRedos = buildingRedo = new Batch(-1);
            redos.addFirst(buildingRedo);
            
            lastRedoPage = -1;
            readCache.clear();
            
            allocator.clear(); 
            baseRevisionFreePages.clear();
            baseRevisionFreePages.add(0, allocator.getLimit());
    
            // Initialize the file header..
            Header h = header();
            h.setByteBufferPosition(0);
            h.magic.set(MAGIC);
            h.base_revision.set(-1);
            h.free_list_page.set(-1);
            h.page_size.set(pageFile.getPageSize());
            h.reserved.set("");
            h.unsynced_redo_page.set(-1);
            replicateHeader();
        }
    }    
    /**
     * Loads an existing file and replays the redo
     * logs to put it in a consistent state.
     */
    public void recover() {
        synchronized (HOUSE_KEEPING_MUTEX) {

            redos.clear();
            performedRedos = syncedRedos = storedRedos = buildingRedo = new Batch(-1);
            redos.addFirst(buildingRedo);
            lastRedoPage = -1;
            readCache.clear();
    
            Header h = header();
            if( !MAGIC.equals( h.magic.get()) ) {
                throw new PagingException("The file header is not of the expected type.");
            }
            
            long baseRevision = h.base_revision.get();
    
            // Initialize the free page list.
            int pageId = h.free_list_page.get();
            if( pageId >= 0 ) {
                baseRevisionFreePages = loadObject(pageId);
                allocator.copy(baseRevisionFreePages);
                Extent.unfree(pageFile, pageId);
            } else {
                allocator.clear(); 
                baseRevisionFreePages.add(0, allocator.getLimit());
            }
            
            boolean consistencyCheckNeeded=true;
            int last_synced_redo = h.redo_page.get();
            pageId = h.unsynced_redo_page.get();
            if( pageId<0 ) {
                pageId = last_synced_redo;
                consistencyCheckNeeded = false;
            }
            while( true ) {
                if( pageId < 0 ) {
                    break;
                }

                if( consistencyCheckNeeded ) {
                    // TODO: when consistencyCheckNeeded==true, then we need to check the
                    // Consistency of the redo, as it may have been partially written to disk.
                }
                
                
                Batch redo = loadObject(pageId); 
                redo.page = pageId;
                redo.recovered = true;
                Extent.unfree(pageFile, pageId);
                
                if( buildingRedo.head == -1 ) {
                    buildingRedo.head = redo.head;
                }
    
                if( baseRevision < redo.head ) {
                    // add first since we are loading redo objects oldest to youngest
                    // but want to put them in the list youngest to oldest.
                    redos.addFirst(redo);
                    performedRedos = syncedRedos = redo;
                    pageId=redo.previous;
                    if( pageId==last_synced_redo ) {
                        consistencyCheckNeeded = false;
                    }
                } else {
                    break;
                }
            }
            
            // Apply all the redos..
            performRedos();
        }        
    }

    /**
     * Once this method returns, any previously committed transactions 
     * are flushed and to the disk, ensuring that they will not be lost
     * upon failure.
     */
    public void flush() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            storeRedos(true);
            syncRedos();
        }
    }   
    
    // /////////////////////////////////////////////////////////////////
    //
    // Methods which transition redos through their life cycle states;
    //
    //    building -> stored -> synced -> performed -> released
    //
    // The HOUSE_KEEPING_MUTEX must be acquired before being called. 
    //
    // /////////////////////////////////////////////////////////////////
    
    /**
     * Attempts to perform a redo state change: building -> stored
     */
    private void storeRedos(boolean force) {
        Batch redo;
        
        // We synchronized /w the transactions so that they see the state change.
        synchronized (TRANSACTION_MUTEX) {
            // Re-checking since storing the redo may not be needed.
            if( (force && buildingRedo.base!=-1 ) || buildingRedo.pageCount() > updateBatchSize ) {
                redo = buildingRedo;
                buildingRedo = new Batch(redo.head);
                redos.addLast(buildingRedo);
            } else {
                return;
            }
        }
        
        // Write any outstanding deferred cache updates...
        redo.performDefferedUpdates(pageFile);

        // Link it to the last redo.
        redo.previous = lastRedoPage; 
        
        // Store the redo record.
        lastRedoPage = redo.page = storeObject(redo);

        // Update the header to know about the new redo page.
        header().unsynced_redo_page.set(redo.page);
        replicateHeader();
    }
    
    /**
     * Performs a file sync. 
     * 
     * This allows two types of redo state changes to occur:
     * <ul>
     * <li> stored -> synced
     * <li> performed -> released
     * </ul>
     */
    private void syncRedos() {

        // This is a slow operation..
        if( synch ) {
            file.sync();
        }
        Header h = header();

        // Update the base_revision with the last performed revision.
        if (performedRedos!=syncedRedos) {
            Batch lastPerformedRedo = syncedRedos.getPrevious();
            h.base_revision.set(lastPerformedRedo.head);
        }

        // Were there some redos in the stored state?
        if (storedRedos!=buildingRedo) {
            
            // The last stored is actually synced now..
            Batch lastStoredRedo = buildingRedo.getPrevious();
            // Let the header know about it..
            h.redo_page.set(lastStoredRedo.page);
            
            // We synchronized /w the transactions so that they see the state change.
            synchronized (TRANSACTION_MUTEX) {
                // Transition stored -> synced.
                storedRedos = buildingRedo;
            }
        }
        
        // Once a redo has been performed, subsequently synced, and no longer referenced,
        // it's allocated recovery space can be released.
        while( performedRedos!=syncedRedos ) {
            if( performedRedos.references!=0 ) {
                break;
            }
            
            // Free the update pages associated with the redo.
            performedRedos.freeRedoSpace(allocator);
            
            // Free the redo record itself.
            Extent.free(pageFile, performedRedos.page);
            
            // don't need to sync /w transactions since they don't use the performedRedos variable.
            // Transition performed -> released
            performedRedos = performedRedos.getNext();
            
            // removes the released redo form the redo list.
            performedRedos.getPrevious().unlink();
        }

        // Store the free list..
        int previousFreeListPage = h.free_list_page.get();
        h.free_list_page.set(storeObject(baseRevisionFreePages));
        replicateHeader();

        // Release the previous free list.
        if (previousFreeListPage >= 0) {
            Extent.free(pageFile, previousFreeListPage);
        }
    }

    /**
     * Attempts to perform a redo state change: synced -> performed
     * 
     * Once a redo is performed, new snapshots will not reference 
     * the redo anymore.
     */
    public void performRedos() {

        if( syncedRedos==storedRedos ) {
            // There are no redos in the synced state for use to transition.
            return;
        }
              
        // The last performed redo MIGHT still have an open snapshot.
        // we can't transition from synced, until that snapshot closes.
        Batch lastPerformed = syncedRedos.getPrevious();
        if( lastPerformed!=null && lastPerformed.references!=0) {
            return;
        }
        
        while( syncedRedos!=storedRedos ) {
            
            // Performing the redo actually applies the updates to the original page locations.
            for (Commit commit : syncedRedos) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                    int page = entry.getKey();
                    Update update = entry.getValue();
                    
                    if( page != update.page ) {
                        
                        if( syncedRedos.recovered ) {
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
                        
                        if( syncedRedos.recovered ) {
                            // If we are recovering, the allocator MIGHT not have this 
                            // page as being allocated.  This makes sure it's allocated so that
                            // new transaction to get this page and overwrite it in error.
                            allocator.unfree(page, 1);
                        }
                        // Update the persistent free list.  This gets stored on the next sync.
                        baseRevisionFreePages.remove(page, 1);
                        
                    } else if( update.wasFreed() ) {
                        baseRevisionFreePages.add(page, 1);
                    }
                }
            }
            
            syncedRedos.performed = true;
            
            // We synchronized /w the transactions so that they see the state change.
            synchronized (TRANSACTION_MUTEX) {
                // Transition synced -> performed
                syncedRedos = syncedRedos.getNext();
            }
            
            lastPerformed = syncedRedos.getPrevious();
            // We have to stop if the last redo performed has an open snapshot.
            if( lastPerformed.references!=0 ) {
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
            BatchEntry entry = buildingRedo.entries.getTail();
            if( entry!=null ) {
                head = entry.isSnapshotHead();
            }
            
            if( head == null ) {
                // create a new snapshot head entry..
                head = new SnapshotHead(this, buildingRedo);
                buildingRedo.entries.addLast(head);
            }
            
            // Open the snapshot off that head position.
            return new Snapshot(this, head, syncedRedos).open();
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
