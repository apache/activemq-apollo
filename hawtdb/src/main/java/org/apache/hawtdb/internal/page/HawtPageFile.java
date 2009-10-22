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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import javolution.io.Struct;

import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.OptimisticUpdateException;
import org.apache.hawtdb.api.Paged;
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

    private static final String MAGIC = "HawtDB:1.0\n";
    private static final int FILE_HEADER_SIZE = 1024 * 4;

    public static final int PAGE_ALLOCATED = -1;
    public static final int PAGE_FREED = -2;
    public static final int HEADER_SIZE = 1024*4;

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

    /**
     * Tracks the page changes that were part of a commit.
     * 
     * Commits can be merged, in that sense this then tracks range of commits.
     * 
     * @author chirino
     */
    static class Commit extends LinkedNode<Commit> {

        /** the redo that this commit is stored in */
        private final Redo redo;        
        /** oldest revision in the commit range. */
        private long base; 
        /** newest revision in the commit range, will match base if this only tracks one commit */ 
        private long head;
        /** set to the open snapshot who's head is this commit */
        private Snapshot snapshot;
        /** all the page updates that are part of the redo */
        private HashMap<Integer, Integer> updates;
        /** the deferred updates that need to be done in this redo */
        private HashMap<Integer, DeferredUpdate> deferredUpdates;

        public Commit(long version, HashMap<Integer, Integer> updates, HashMap<Integer, DeferredUpdate> deferredUpdates, Redo redo) {
            this.redo = redo;
            this.head = this.base = version;
            this.updates = updates;
            this.deferredUpdates = deferredUpdates;
        }
        
        public String toString() { 
            int updateSize = updates==null ? 0 : updates.size();
            int cacheSize = deferredUpdates==null ? 0 : deferredUpdates.size();
            return "{ base: "+this.base+", head: "+this.head+", updates: "+updateSize+", cache: "+cacheSize+" }";
        }

        public long commitCheck(HashMap<Integer, Integer> newUpdate) {
            for (Integer page : newUpdate.keySet()) {
                if( updates.containsKey( page ) ) {
                    throw new OptimisticUpdateException();
                }
            }
            return head;
        }

        public void putAll(HashMap<Integer, Integer> udpates, HashMap<Integer, DeferredUpdate> deferredUpdates) {
            if( udpates!=null ) {
                if( this.updates == null ) {
                    this.updates = udpates;
                } else {
                    this.updates.putAll(udpates);
                }
            }
            if( deferredUpdates!=null ) {
                if( this.deferredUpdates == null ) {
                    this.deferredUpdates = deferredUpdates;
                } else {
                    this.deferredUpdates.putAll(deferredUpdates);
                }
            }
        }
        
    }
    
    /**
     * Aggregates a group of commits so that they can be more efficiently operated against.
     * 
     */
    static private class Redo extends LinkedNode<Redo> implements Externalizable {
        private static final long serialVersionUID = 1188640492489990493L;
        
        /** the pageId that this redo batch is stored at */
        private transient int page=-1;
        /** points to a previous redo batch page */
        public int previous=-1;
        /** was the redo loaded in the {@link recover} method */
        private transient boolean recovered;
        /** the commits stored in the redo */ 
        private transient LinkedNodeList<Commit> commits = new LinkedNodeList<Commit>();
        /** all the page updates that are part of the redo */
        private ConcurrentHashMap<Integer, Integer> updates = new ConcurrentHashMap<Integer, Integer>();
        /** the deferred updates that need to be done in this redo */
        private transient ConcurrentHashMap<Integer, DeferredUpdate> deferredUpdates = new ConcurrentHashMap<Integer, DeferredUpdate>();
        /** tracks how many snapshots are referencing the redo */
        private int references;
        /** set to the open snapshot who's head is before this redo */
        private Snapshot prevSnapshot;
        /** set to the open snapshot who's head is this redo */
        private Snapshot snapshot;
        /** the oldest commit in this redo */
        public long base=-1;
        /** the newest commit in this redo */
        public long head;
        
        @SuppressWarnings("unused")
        public Redo() {
        }
        
        public Redo(long head) {
            this.head = head;
        }

        public String toString() { 
            int count = updates==null ? 0 : updates.size();
            return "{ page: "+this.page+", updates: "+count+", previous: "+previous+" }";
        }
        
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(head);
            out.writeLong(base);
            out.writeInt(previous);
            out.writeObject(updates);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            head = in.readLong();
            base = in.readLong();
            previous = in.readInt();
            updates = (ConcurrentHashMap<Integer, Integer>) in.readObject();
        }        

        public int pageCount() {
            int rc = 0;
            rc = updates.size();
            // TODO: we can probably get an idea of how many pages the deferred update will use.
            // rc += deferredUpdates.size();
            return rc;
        }

        public long commitCheck(HashMap<Integer, Integer> newUpdate) {
            for (Integer page : newUpdate.keySet()) {
                if( updates.containsKey( page ) ) {
                    throw new OptimisticUpdateException();
                }
            }
            return head;
        }

        public void putAll(HashMap<Integer, Integer> updates, HashMap<Integer, DeferredUpdate> deferredUpdates) {
            if( updates!=null ) {
                this.updates.putAll(updates);
            }
            if( deferredUpdates!=null ) {
                this.deferredUpdates.putAll(deferredUpdates);
            }
        }

    }

    /**
     * Provides a snapshot view of the page file.
     *  
     * @author chirino
     */
    abstract class Snapshot {
        /** The number of transactions that are holding this snapshot open */
        protected int references;
        
        public String toString() { 
            return "{ references: "+this.references+" }";
        }

        public int mapPageId(int page) {
            return page;
        }
        
        public <T> T cacheLoad(EncoderDecoder<T> marshaller, int page) {
            return readCache.cacheLoad(marshaller, page);
        }
        
        public void read(int pageId, Buffer buffer) throws IOPagingException {
            pageId = mapPageId(pageId);
            pageFile.read(pageId, buffer);
        }

        public ByteBuffer slice(int pageId, int count) {
            pageId = mapPageId(pageId);
            return pageFile.slice(SliceType.READ, pageId, count);
        }
        
        abstract public Snapshot open();
        abstract public void close();
        abstract public long commitCheck(HashMap<Integer, Integer> pageUpdates);
    }
    
    class PreviousSnapshot extends Snapshot {
        private final Redo redo;

        public PreviousSnapshot(Redo redo) {
            this.redo = redo;
        }
        
        @Override
        public Snapshot open() {
            references++;
            if( references==1 ) {
                redo.references++;
                redo.prevSnapshot = this;
            }
            return this;
        }

        @Override
        public void close() {
            references--;
            if( references==0 ) {
                redo.references--;
                redo.prevSnapshot = null;
            }
        }
        
        public long commitCheck(HashMap<Integer, Integer> pageUpdates) {
            long rc = 0;
            Redo cur = redo;
            while (cur != null) {
                rc = cur.commitCheck(pageUpdates);
                cur = cur.getNext();
            }
            return rc;
        }

    }

    class RedosSnapshot extends Snapshot {
        
        /** The snapshot will load page updates from the following redo list. */
        protected final List<Redo> redosInSnapshot;
        
        public RedosSnapshot() {
            this.redosInSnapshot = snapshotRedos();
        }
        
        public Snapshot open() {
            references++;
            if( references==1 ) {
                for (Redo redo : redosInSnapshot) {
                    redo.references++;
                }
                redosInSnapshot.get(0).snapshot = this;
            }
            return this;
        }

        public void close() {
            references--;
            if( references==0 ) {
                for (Redo redo : redosInSnapshot) {
                    redo.references--;
                }
                redosInSnapshot.get(0).snapshot = null;
            }
        }
        
        public int mapPageId(int page) {
            // It may be in the redos..
            for (Redo redo : redosInSnapshot) {
                Integer updatedPage = redo.updates.get(page);
                if (updatedPage != null) {
                    switch (updatedPage) {
                    case PAGE_FREED:
                        throw new PagingException("You should never try to read page that has been freed.");
                    case PAGE_ALLOCATED:
                        return page;
                    default:
                        return updatedPage;
                    }
                }
            }
            
            return super.mapPageId(page);
        }
        
        public <T> T cacheLoad(EncoderDecoder<T> marshaller, int page) {
            for (Redo redo : redosInSnapshot) {
                DeferredUpdate cu  = redo.deferredUpdates.get(page);
                if (cu != null) {
                    return cu.<T>value();
                }
            }
            return super.cacheLoad(marshaller, page);
        }
        
        public long commitCheck(HashMap<Integer, Integer> pageUpdates) {
            long rc = 0;
            Redo cur = redosInSnapshot.get(0).getNext();
            while (cur != null) {
                rc = cur.commitCheck(pageUpdates);
                cur = cur.getNext();
            }
            return rc;
        }
        
        
    }
    
    class CommitsSnapshot extends RedosSnapshot {
        /** The snapshot will load page updates from the following commit and all it's previous linked commits. */
        private final Commit commit;

        public CommitsSnapshot(Commit commit) {
            this.commit= commit;
        }

        public Snapshot open() {
            references++;
            if( references==1 ) {
                for (Redo redo : redosInSnapshot) {
                    redo.references++;
                }
                commit.redo.references++;
                commit.snapshot = this;
            }
            return this;
        }
        
        public void close() {
            references--;
            if( references==0 ) {
                for (Redo redo : redosInSnapshot) {
                    redo.references--;
                }
                commit.redo.references--;
                commit.snapshot = null;
            }
        }


        public int mapPageId(int page) {
            
            // Check to see if it's in the current update list...
            Commit update = this.commit;
            while (update!=null) {
                Integer updatedPage = update.updates.get(page);
                if (updatedPage != null) {
                    switch (updatedPage) {
                    case PAGE_FREED:
                        throw new PagingException("You should never try to read page that has been freed.");
                    case PAGE_ALLOCATED:
                        return page;
                    default:
                        return updatedPage;
                    }
                }
                update = update.getPrevious();
            }
            
            return super.mapPageId(page);
        }

        public <T> T cacheLoad(EncoderDecoder<T> marshaller, int page) {
            // Check to see if it's in the current update list...
            Commit update = this.commit;
            while (update!=null) {
                DeferredUpdate du = update.deferredUpdates.get(page);
                if (du != null) {
                    return du.<T>value();
                }
                update = update.getPrevious();
            }
            
            return super.cacheLoad(marshaller, page);
        }
        
        public long commitCheck(HashMap<Integer, Integer> pageUpdates) {
            long rc = 0;
            
            Commit next = this.commit.getNext();
            while (next != null) {
                rc = next.commitCheck(pageUpdates);
                next = next.getNext();
            }
            
            Redo cur = this.commit.redo.getNext();
            while (cur != null) {
                rc = cur.commitCheck(pageUpdates);
                cur = cur.getNext();
            }
            return rc;
        }
        
    }
    
    private final MemoryMappedFile file;
    final SimpleAllocator allocator;
    final PageFile pageFile;
    private static final int updateBatchSize = 1024;


    /** The header structure of the file */
    private final Header header = new Header();
    
    int lastRedoPage = -1;
    
    private final LinkedNodeList<Redo> redos = new LinkedNodeList<Redo>();
    
    //
    // The following Redo objects point to linked nodes in the previous redo list.  
    // They are used to track designate the state of the redo object.
    //
    
    /** The current redo that is currently being built */
    Redo buildingRedo;
    /** The stored redos.  These might be be recoverable. */
    Redo storedRedos;
    /** The synced redos.  A file sync occurred after these redos were stored. */
    Redo syncedRedos;
    /** The performed redos.  Updates are actually performed to the original page file. */
    Redo performedRedos;
    
    /** Used as cache read objects */
    private ReadCache readCache = new ReadCache();

    /** Mutex for data structures which are used during house keeping tasks like redo management. Once acquired, you can also acquire the TRANSACTION_MUTEX */
    private final Object HOUSE_KEEPING_MUTEX = "HOUSE_KEEPING_MUTEX";

    /** Mutex for data structures which transaction threads access. Never attempt to acquire the HOUSE_KEEPING_MUTEX once this mutex is acquired.  */
    private final Object TRANSACTION_MUTEX = "TRANSACTION_MUTEX";
    

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
    
    public HawtPageFile(PageFile pageFile) {
        this.pageFile = pageFile;
        this.file = pageFile.getFile();
        this.allocator = pageFile.allocator();
        ByteBuffer slice = file.slice(false, 0, FILE_HEADER_SIZE);
        this.header.setByteBuffer(slice, slice.position());
    }

    public Transaction tx() {
        return new HawtTransaction(this);
    }

    /**
     * Attempts to commit a set of page updates.
     * 
     * @param updatedSnapshot
     * @param pageUpdates
     * @param deferredUpdates
     */
    void commit(Snapshot updatedSnapshot, HashMap<Integer, Integer> pageUpdates, HashMap<Integer, DeferredUpdate> deferredUpdates) {
        
        boolean fullRedo=false;
        synchronized (TRANSACTION_MUTEX) {
            
            // we need to figure out the revision id of the this commit...
            long rev;
            if( updatedSnapshot!=null ) {
                
                // Lets check for an OptimisticUpdateException
                // verify that the new commit's updates don't conflict with a commit that occurred
                // subsequent to the snapshot that this commit started operating on.
                
                // Note: every deferred update has an entry in the pageUpdates, so no need to 
                // check to see if that map also conflicts.
                rev = updatedSnapshot.commitCheck(pageUpdates);
                updatedSnapshot.close();
            } else {
                rev = buildingRedo.head;
            }
            rev++;

            if( buildingRedo.base == -1 ) {
                buildingRedo.base = rev;
            }

            buildingRedo.head = rev;
            
            // TODO: This map merging has to be a bit CPU intensive.. need 
            // to look for ways to optimize it out.
            buildingRedo.putAll(pageUpdates, deferredUpdates);
            
            Commit last = buildingRedo.commits.getTail();
            if( last==null || last.snapshot!=null ) {
                last = new Commit(rev, pageUpdates, deferredUpdates, buildingRedo);
                buildingRedo.commits.addLast(last);
            } else {
                // we can merge into the previous commit.
                last.putAll(pageUpdates, deferredUpdates);
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
            performedRedos = syncedRedos = storedRedos = buildingRedo = new Redo(-1);
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
            h.base_revision.set(0);
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
            performedRedos = syncedRedos = storedRedos = buildingRedo = new Redo(-1);
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
            
            // Load the redo batches.
            pageId = h.unsynced_redo_page.get();
            while( pageId >= 0 ) {
                Redo redo = loadObject(pageId); 
                redo.page = pageId;
                redo.recovered = true;
                Extent.unfree(pageFile, pageId);
                
                if( buildingRedo.head == -1 ) {
                    buildingRedo.head = redo.head;
                }
    
                pageId=-1;
                if( baseRevision < redo.head ) {
                    
                    // add first since we are loading redo objects oldest to youngest
                    // but want to put them in the list youngest to oldest.
                    redos.addFirst(redo);
                    syncedRedos = redo;
                    
                    if( baseRevision < redo.base ) {
                        pageId=redo.previous;
                    }
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
        Redo redo;
        
        // We synchronized /w the transactions so that they see the state change.
        synchronized (TRANSACTION_MUTEX) {
            // Re-checking since storing the redo may not be needed.
            if( (force && buildingRedo.base!=-1 ) || buildingRedo.pageCount() > updateBatchSize ) {
                redo = buildingRedo;
                buildingRedo = new Redo(redo.head);
                redos.addLast(buildingRedo);
            } else {
                return;
            }
        }
        
        // Write any outstanding deferred cache updates...
        if( redo.deferredUpdates != null ) {
            for (Entry<Integer, DeferredUpdate> entry : redo.deferredUpdates.entrySet()) {
                DeferredUpdate cu = entry.getValue();
                List<Integer> allocatedPages = cu.store(pageFile);
                for (Integer page : allocatedPages) {
                    // add any allocated pages to the update list so that the free 
                    // list gets properly adjusted.
                    redo.updates.put(page, PAGE_ALLOCATED);
                }
            }
        }

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
        file.sync();
        Header h = header();

        // Update the base_revision with the last performed revision.
        if (performedRedos!=syncedRedos) {
            Redo lastPerformedRedo = syncedRedos.getPrevious();
            h.base_revision.set(lastPerformedRedo.head);
        }

        // Were there some redos in the stored state?
        if (storedRedos!=buildingRedo) {
            
            // The last stored is actually synced now..
            Redo lastStoredRedo = buildingRedo.getPrevious();
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
            for (Entry<Integer, Integer> entry : performedRedos.updates.entrySet()) {
                int key = entry.getKey();
                int value = entry.getValue();
        
                switch( value ) {
                case PAGE_ALLOCATED:
                    // It was a new page that was written.. we don't need to 
                    // free it.
                    break;
                case PAGE_FREED:
                    // update freed a previous page.. now is when we actually free it.
                    allocator.free(key, 1);
                    break;
                default:
                    // This updated the 'key' page, now we can release the 'value' page
                    // since it has been copied over the 'key' page and is no longer needed.
                    allocator.free(value, 1);
                }
            }
            
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
        Redo lastPerformed = syncedRedos.getPrevious();
        if( lastPerformed!=null && lastPerformed.references!=0) {
            return;
        }
        
        while( syncedRedos!=storedRedos ) {
            
            // Performing the redo actually applies the updates to the original page locations. 
            for (Entry<Integer, Integer> entry : syncedRedos.updates.entrySet()) {
                int key = entry.getKey();
                int value = entry.getValue();
                switch( value ) {
                case PAGE_ALLOCATED:
                    if( syncedRedos.recovered ) {
                        // If we are recovering, the allocator MIGHT not have this 
                        // page as being allocated.  This makes sure it's allocated so that
                        // new transaction to get this page and overwrite it in error.
                        allocator.unfree(key, 1);
                    }
                    // Update the persistent free list.  This gets stored on the next sync.
                    baseRevisionFreePages.remove(key, 1);
                    break;
                case PAGE_FREED:
                    // The actual free gets done on the next file sync.
                    // Update the persistent free list.  This gets stored on the next sync.
                    baseRevisionFreePages.add(key, 1);
                    break;
                default:
                    if( syncedRedos.recovered ) {
                        // If we are recovering, the allocator MIGHT not have this 
                        // page as being allocated.  This makes sure it's allocated so that
                        // new transaction to get this page and overwrite it in error.
                        allocator.unfree(key, 1);
                    }
                    
                    // Perform the update by copying the updated 'redo page' to the original
                    // page location.
                    ByteBuffer slice = pageFile.slice(SliceType.READ, value, 1);
                    try {
                        pageFile.write(key, slice);
                    } finally { 
                        pageFile.unslice(slice);
                    }
                }
            }
            
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
            
            // Is it a partial redo snapshot??  
            // If there are commits in the next redo..
            Snapshot snapshot;
            Commit commit = buildingRedo.commits.getTail();
            if( commit!=null ) {
                snapshot = commit.snapshot != null ? commit.snapshot : new CommitsSnapshot(commit);
                return snapshot.open();
            }

            // Perhaps this snapshot has to deal with full redos..
            if( syncedRedos!=buildingRedo ) {
                Redo lastRedo = buildingRedo.getPrevious();
                snapshot = lastRedo.snapshot != null ? lastRedo.snapshot : new RedosSnapshot();
                return snapshot.open();
            }
            
            // Then the snapshot does not have previous updates.
            snapshot = buildingRedo.prevSnapshot != null ? buildingRedo.prevSnapshot : new PreviousSnapshot(buildingRedo);
            return snapshot.open();
        }
    }
    
    private List<Redo> snapshotRedos() {
        if( syncedRedos!=buildingRedo ) {
            ArrayList<Redo> rc = new ArrayList<Redo>(4);
            Redo cur = buildingRedo.getPrevious();
            while( true ) {
                rc.add(cur);
                if( cur == syncedRedos ) {
                    break;
                }
                cur = cur.getPrevious();
            }
            return rc;
        } else {
            return Collections.emptyList();
        }
    }

    void closeSnapshot(Snapshot snapshot) {
        if( snapshot!=null ) {
            synchronized(TRANSACTION_MUTEX) {
                snapshot.close();
            }
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

    class ReadCache {
        private final Map<Integer, Object> map = Collections.synchronizedMap(new LRUCache<Integer, Object>(1024));

        @SuppressWarnings("unchecked")
        private <T> T cacheLoad(EncoderDecoder<T> marshaller, int pageId) {
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
    
    static class DeferredUpdate {
        private final int page;
        private Object value;
        private EncoderDecoder<?> marshaller;

        public DeferredUpdate(int page, Object value, EncoderDecoder<?> marshaller) {
            this.page = page;
            this.value = value;
            this.marshaller = marshaller;
        }

        public void reset(Object value, EncoderDecoder<?> marshaller) {
            this.value = value;
            this.marshaller = marshaller;
        }

        @SuppressWarnings("unchecked")
        <T> T value() {
            return (T) value;
        }
        
        @SuppressWarnings("unchecked")
        public List<Integer> store(Paged paged) {
            return ((EncoderDecoder)marshaller).store(paged, page, value);
        }
    }}
