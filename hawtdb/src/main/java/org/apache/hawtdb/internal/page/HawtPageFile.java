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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import javolution.io.Struct;

import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;
import org.apache.hawtdb.api.Allocator;
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

    abstract static class RedoEntry extends LinkedNode<RedoEntry> {
        Commit isCommit() {
            return null;
        }
        SnapshotHead isSnapshotHead() {
            return null;
        }
    }
    
    /**
     * Tracks the page changes that were part of a commit.
     * 
     * Commits can be merged, in that sense this then tracks range of commits.
     * 
     * @author chirino
     */
    final static class Commit extends RedoEntry implements Externalizable {

        /** oldest revision in the commit range. */
        private long base; 
        /** newest revision in the commit range, will match base if this only tracks one commit */ 
        private long head;
        /** all the page updates that are part of the redo */
        private ConcurrentHashMap<Integer, Update> updates;
        /** the deferred updates that need to be done in this redo */
        private ConcurrentHashMap<Integer, DeferredUpdate> deferredUpdates;


        public Commit() {
        }
        
        public Commit(long version, ConcurrentHashMap<Integer, Update> updates, ConcurrentHashMap<Integer, DeferredUpdate> deferredUpdates) {
            this.head = this.base = version;
            this.updates = updates;
            this.deferredUpdates = deferredUpdates;
        }
        
        
        @Override
        Commit isCommit() {
            return this;
        }

        
        public String toString() { 
            int updateSize = updates==null ? 0 : updates.size();
            int cacheSize = deferredUpdates==null ? 0 : deferredUpdates.size();
            return "{ base: "+this.base+", head: "+this.head+", updates: "+updateSize+", cache: "+cacheSize+" }";
        }

        public long commitCheck(Map<Integer, Update> newUpdate) {
            for (Integer page : newUpdate.keySet()) {
                if( updates.containsKey( page ) ) {
                    throw new OptimisticUpdateException();
                }
            }
            return head;
        }

        public void merge(Allocator allocator, long rev, ConcurrentHashMap<Integer, Update> updates, ConcurrentHashMap<Integer, DeferredUpdate> deferredUpdates) {
            assert head+1 == rev;
            head=rev;
            if (deferredUpdates != null) {
                if (this.deferredUpdates == null) {
                    this.deferredUpdates = deferredUpdates;
                } else {
                    for (Entry<Integer, DeferredUpdate> entry : deferredUpdates.entrySet()) {
                        Integer page = entry.getKey();
                        DeferredUpdate du = entry.getValue();
                        if (du.value == null) {
                            this.deferredUpdates.remove(page);
                        } else {
                            DeferredUpdate previous = this.deferredUpdates.put(page, du);
                            // TODO: There was a previous deferred update in the redo...  we can just use it's 
                            // redo allocation and release the new allocation.
                            if (previous != null) {
                                Update allocated = updates.remove(page);
                                assert allocated.update_location == du.page; // these should match...
                                allocator.free(du.page, 1);
                                // since we replaced the previous entry,  
                                du.page = previous.page;
                            }
                        }
                    }
                }
            }
            
            // merge all the entries in the update..
            for (Entry<Integer, Update> entry : updates.entrySet()) {
                merge(allocator, entry.getKey(), entry.getValue());
            }
        }

        /**
         * merges one update..
         * 
         * @param page
         * @param update
         */
        private void merge(Allocator allocator, int page, Update update) {
            Update previous = this.updates.put(page, update);
            if (previous != null) {
                if( update.wasFreed() ) {
                    // we can undo the previous update
                    if( previous.update_location != page ) {
                        allocator.free(previous.update_location, 1);
                    }
                    if( previous.wasAllocated() ) {
                        allocator.free(page, 1);
                    }
                    this.updates.remove(page);
                } else {
                    // we are undoing the previous update /w this new update.
                    if( previous.update_location != page ) {
                        allocator.free(previous.update_location, 1);
                    }
                    // we may be updating a previously allocated page,
                    // if so we need to mark the new page as allocated too.
                    if( previous.wasAllocated() ) {
                        update.flags = PAGE_ALLOCATED;
                    }                    
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            base = in.readLong();
            head = in.readLong();
            updates = (ConcurrentHashMap<Integer, Update>) in.readObject();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(base);
            out.writeLong(head);
            out.writeObject(updates);
        }
        
    }
    
    final class Snapshot {
        final SnapshotHead head;
        final Redo base;
        
        public Snapshot(SnapshotHead head, Redo base) {
            this.head = head;
            this.base = base;
        }

        public Snapshot open() {
            head.open(base);
            return this;
        }
        
        public void close() {
            synchronized(TRANSACTION_MUTEX) {
                head.close(base);
            }
        }
    }
    
    /**
     * Provides a snapshot view of the page file.
     *  
     * @author chirino
     */
    final class SnapshotHead extends RedoEntry {
        final Redo parent;
        
        public SnapshotHead(Redo parent) {
            this.parent = parent;
        }

        /** The number of times this snapshot has been opened. */
        protected int references;
        
        public String toString() { 
            return "{ references: "+this.references+" }";
        }

        SnapshotHead isSnapshotHead() {
            return this;
        }
        
        public void read(int pageId, Buffer buffer) throws IOPagingException {
            pageId = mapPageId(pageId);
            pageFile.read(pageId, buffer);
        }

        public ByteBuffer slice(int pageId, int count) {
            pageId = mapPageId(pageId);
            return pageFile.slice(SliceType.READ, pageId, count);
        }
        
        public void open(Redo base) {
            references++;
            while( true ) {
                base.references++;
                if(base == parent ) {
                    break;
                }
                base = base.getNext();
            }
        }
        
        public void close(Redo base) {
            references--;
            while( true ) {
                base.references--;
                if(base == parent ) {
                    break;
                }
                base = base.getNext();
            }

            if( references==0 ) {
                unlink();
                // TODO: trigger merging of adjacent commits. 
            }
        }

        public int mapPageId(int page) {
            // Look for the page in the previous commits..
            Redo curRedo = parent;
            RedoEntry curEntry = getPrevious();
            while( true ) {
                if( curRedo.isPerformed() ) {
                    break;
                }
                
                while( curEntry!=null ) {
                    Commit commit = curEntry.isCommit();
                    if( commit !=null ) {
                        Update update = commit.updates.get(page);
                        if( update!=null ) {
                            return update.page();
                        }
                    }
                    curEntry = curEntry.getPrevious();
                }
                
                curRedo = curRedo.getPrevious();
                if( curRedo==null ) {
                    break;
                }
                curEntry = curRedo.entries.getTail();
            }
            return page;
        }
        
        
        public <T> T cacheLoad(EncoderDecoder<T> marshaller, int page) {
            Redo curRedo = parent;
            RedoEntry curEntry = getPrevious();
            while( true ) {
                if( curRedo.isPerformed() ) {
                    break;
                }
                
                while( curEntry!=null ) {
                    Commit commit = curEntry.isCommit();
                    if( commit !=null ) {
                        DeferredUpdate du  = commit.deferredUpdates.get(page);
                        if (du!=null) {
                            return du.<T>value();
                        }
                    }
                    curEntry = curEntry.getPrevious();
                }
                
                curRedo = curRedo.getPrevious();
                if( curRedo==null ) {
                    break;
                }
                curEntry = curRedo.entries.getTail();
            }
            return readCache.cacheLoad(marshaller, page);
        }
        
        public long commitCheck(Map<Integer, Update> pageUpdates) {
            long rc=0;
            Redo curRedo = parent;
            RedoEntry curEntry = getNext();
            while( true ) {
                while( curEntry!=null ) {
                    Commit commit = curEntry.isCommit();
                    if( commit!=null ) {
                        rc = commit.commitCheck(pageUpdates);
                    }
                    curEntry = curEntry.getNext();
                }
                
                curRedo = curRedo.getNext();
                if( curRedo==null ) {
                    break;
                }
                curEntry = curRedo.entries.getHead();
            }
            return rc;
        }
                
    }
    
    /**
     * Aggregates a group of commits so that they can be more efficiently operated against.
     * 
     */
    static class Redo extends LinkedNode<Redo> implements Externalizable, Iterable<Commit> {
        private static final long serialVersionUID = 1188640492489990493L;
        
        /** the pageId that this redo batch is stored at */
        private transient int page=-1;
        /** points to a previous redo batch page */
        public int previous=-1;
        /** was the redo loaded in the {@link recover} method */
        private transient boolean recovered;
        
        /** the commits and snapshots in the redo */ 
        private transient LinkedNodeList<RedoEntry> entries = new LinkedNodeList<RedoEntry>();
        /** tracks how many snapshots are referencing the redo */
        private int references;

        /** the oldest commit in this redo */
        public long base=-1;
        /** the newest commit in this redo */
        public long head;

        private boolean performed;
        
        public Redo() {
        }
        
        public boolean isPerformed() {
            return performed;
        }

        public Redo(long head) {
            this.head = head;
        }

        public String toString() { 
            return "{ page: "+this.page+", previous: "+previous+" }";
        }
        
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(head);
            out.writeLong(base);
            out.writeInt(previous);

            // Only need to store the commits.
            ArrayList<Commit> l = new ArrayList<Commit>();
            for (Commit commit : this) {
                l.add(commit);
            }
            out.writeObject(l);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            head = in.readLong();
            base = in.readLong();
            previous = in.readInt();
            ArrayList<Commit> l = (ArrayList<Commit>) in.readObject();
            for (Commit commit : l) {
                entries.addLast(commit);
            }
        }        

        public int pageCount() {
            int rc = 0;
            for (Commit commit : this) {
                rc += commit.updates.size();
            }
            return rc;
        }
        
        @Override
        public Iterator<Commit> iterator() {
            return new Iterator<Commit>() {
                Commit next = nextCommit(entries.getHead());
                Commit last;
                
                @Override
                public boolean hasNext() {
                    return next!=null;
                }

                @Override
                public Commit next() {
                    if( next==null ) {
                        throw new NoSuchElementException();
                    }
                    last = next;
                    next = nextCommit(next.getNext());
                    return last;
                }

                @Override
                public void remove() {
                    if( last==null ) {
                        throw new IllegalStateException();
                    }
                    last.unlink();
                }
            };
        }


        private Commit nextCommit(RedoEntry entry) {
            while( entry != null ) {
                Commit commit = entry.isCommit();
                if( commit!=null ) {
                    return commit;
                }
                entry = entry.getNext();
            }
            return null;
        }

        public void performDefferedUpdates(Paged pageFile) {            
            for (Commit commit : this) {
                if( commit.deferredUpdates != null ) {
                    for (Entry<Integer, DeferredUpdate> entry : commit.deferredUpdates.entrySet()) {
                        DeferredUpdate cu = entry.getValue();
                        if( cu.value == null ) {
                            List<Integer> freePages = cu.marshaller.remove(pageFile, cu.page);
                            for (Integer page : freePages) {
                                commit.merge(pageFile.allocator(), page, Update.freed(page));
                            }
                        } else {
                            List<Integer> allocatedPages = cu.store(pageFile);
                            for (Integer page : allocatedPages) {
                                // add any allocated pages to the update list so that the free 
                                // list gets properly adjusted.
                                commit.merge(pageFile.allocator(), page, Update.allocated(page));
                            }
                        }
                    }
                }
            }
        }

        public void freeRedoSpace(SimpleAllocator allocator) {
            for (Commit commit : this) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                    int key = entry.getKey();
                    Update value = entry.getValue();
                    if( value.wasFreed() ) {
                        allocator.free(key, 1);
                    } else if( key != value.update_location ) {
                        // need to free the udpate page..
                        allocator.free(value.update_location, 1);
                    }
                }
            }
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
    
    /** Used as read cache */
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
        return "{ allocator: "+allocator
        +", synch: "+synch
        +", read cache size: "+readCache.map.size()
        +", base revision free pages: "+baseRevisionFreePages + ",\n"
        + "  redos: {\n" 
        + "    performed: "+toString(performedRedos, syncedRedos) + ",\n" 
        + "    synced: "+toString(syncedRedos, storedRedos) + ",\n" 
        + "    stored: "+toString(storedRedos, buildingRedo)+ ",\n" 
        + "    building: "+toString(buildingRedo, null)+ ",\n"
        + "  }"        
        + "}";
    }

    /** 
     * @param from
     * @param to
     * @return string representation of the redo items from the specified redo up to (exclusive) the specified redo.
     */
    private String toString(Redo from, Redo to) {
        StringBuilder rc = new StringBuilder();
        rc.append("[ ");
        Redo t = from;
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
    void commit(Snapshot snapshot, ConcurrentHashMap<Integer, Update> pageUpdates, ConcurrentHashMap<Integer, DeferredUpdate> deferredUpdates) {
        
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
                rev = snapshot.head.commitCheck(pageUpdates);
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
            RedoEntry last = buildingRedo.entries.getTail();
            if( last!=null ) {
                commit = last.isCommit();
            }
            
            if( commit!=null ) {
                // TODO: figure out how to do the merge outside the TRANSACTION_MUTEX
                commit.merge(pageFile.allocator(), rev, pageUpdates, deferredUpdates);
            } else {
                buildingRedo.entries.addLast(new Commit(rev, pageUpdates, deferredUpdates) );
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
        Redo lastPerformed = syncedRedos.getPrevious();
        if( lastPerformed!=null && lastPerformed.references!=0) {
            return;
        }
        
        while( syncedRedos!=storedRedos ) {
            
            // Performing the redo actually applies the updates to the original page locations.
            for (Commit commit : syncedRedos) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                    int page = entry.getKey();
                    Update update = entry.getValue();
                    
                    if( page != update.update_location ) {
                        
                        if( syncedRedos.recovered ) {
                            // If we are recovering, the allocator MIGHT not have this 
                            // page as being allocated.  This makes sure it's allocated so that
                            // new transaction to get this page and overwrite it in error.
                            allocator.unfree(page, 1);
                        }
                        
                        // Perform the update by copying the updated page the original
                        // page location.
                        ByteBuffer slice = pageFile.slice(SliceType.READ, update.update_location, 1);
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
            RedoEntry entry = buildingRedo.entries.getTail();
            if( entry!=null ) {
                head = entry.isSnapshotHead();
            }
            
            if( head == null ) {
                // create a new snapshot head entry..
                head = new SnapshotHead(buildingRedo);
                buildingRedo.entries.addLast(head);
            }
            
            // Open the snapshot off that head position.
            return new Snapshot(head, syncedRedos).open();
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
    
    final static class DeferredUpdate {
        int page;
        Object value;
        EncoderDecoder<?> marshaller;

        public DeferredUpdate(int page, Object value, EncoderDecoder<?> marshaller) {
            this.page = page;
            this.value = value;
            this.marshaller = marshaller;
        }
        
        @Override
        public String toString() {
            return "{ page: "+page+", removed: "+(value==null)+" }";
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
    }
    
    public static final byte PAGE_ALLOCATED = 1;
    public static final byte PAGE_FREED = 2;
    
    final static class Update implements Serializable {

        private static final long serialVersionUID = -1128410792448869134L;
        
        byte flags;
        final int update_location;
       
        public Update(int updateLocation, byte flags) {
            this.update_location = updateLocation;
            this.flags = flags;
        }

        public static Update updated(int page) {
            return new Update(page, (byte) 0);
        }

        public static Update allocated(int page) {
            return new Update(page, PAGE_ALLOCATED);
        }

        public static Update freed(int page) {
            return new Update(page, PAGE_FREED);
        }

        public boolean wasFreed() {
            return flags == PAGE_FREED;
        }
        
        public boolean wasAllocated() {
            return flags == PAGE_ALLOCATED;
        }
        
        public int page() {
            if( wasFreed() ) {
                throw new PagingException("You should never try to read or write page that has been freed.");
            }
            return update_location;
        }

        @Override
        public String toString() {
            return "{ page: "+update_location+", flags: "+flags+" }";
        }

    }
    
}
