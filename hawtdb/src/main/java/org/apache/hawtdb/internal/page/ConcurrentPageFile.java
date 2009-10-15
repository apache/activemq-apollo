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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.hawtdb.api.OutOfSpaceException;
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
public final class ConcurrentPageFile {

    private static final String MAGIC = "HawtDB:MVCC Page File:1.0\n";
    private static final int FILE_HEADER_SIZE = 1024 * 4;

    public static final int PAGE_ALLOCATED = -1;
    public static final int PAGE_FREED = -2;
//    public static final int PAGE_CACHED_WRITE = -3;
    public static final int HEADER_SIZE = 1024*4;

    static class CacheUpdate {
        private final int page;
        private Object value;
        private EncoderDecoder<?> marshaller;

        public CacheUpdate(int page, Object value, EncoderDecoder<?> marshaller) {
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
    }
    
    /**
     * Transaction objects are NOT thread safe. Users of this object should
     * guard it from concurrent access.
     * 
     * @author chirino
     */
    private final class ConcurrentTransaction implements Transaction {
        private HashMap<Integer, CacheUpdate> cache;
        private HashMap<Integer, Integer> updates;
        private Snapshot snapshot;
        
        private final Allocator txallocator = new Allocator() {
            
            public void free(int pageId, int count) {
                // TODO: this is not a very efficient way to handle allocation ranges.
                int end = pageId+count;
                for (int key = pageId; key < end; key++) {
                    Integer previous = getUpdates().put(key, PAGE_FREED);
                    
                    // If it was an allocation that was done in this
                    // tx, then we can directly release it.
                    assert previous!=null;
                    if( previous == PAGE_ALLOCATED) {
                        getUpdates().remove(key);
                        allocator.free(key, 1);
                    }
                }
            }
            
            public int alloc(int count) throws OutOfSpaceException {
                int pageId = allocator.alloc(count);
                // TODO: this is not a very efficient way to handle allocation ranges.
                int end = pageId+count;
                for (int key = pageId; key < end; key++) {
                    getUpdates().put(key, PAGE_ALLOCATED);
                }
                return pageId;
            }

            public void unfree(int pageId, int count) {
                throw new UnsupportedOperationException();
            }
            
            public void clear() throws UnsupportedOperationException {
                throw new UnsupportedOperationException();
            }

            public int getLimit() {
                return allocator.getLimit();
            }

            public boolean isAllocated(int page) {
                return allocator.isAllocated(page);
            }

        };

        public <T> T get(EncoderDecoder<T> marshaller, int page) {
            // Perhaps the page was updated in the current transaction...
            CacheUpdate rc = cache == null ? null : cache.get(page);
            if( rc != null ) {
                return rc.<T>value();
            }
            
            // No?  Then ask the snapshot to load the object.
            return snapshot().cacheLoad(marshaller, page);
        }

        public <T> void put(EncoderDecoder<T> marshaller, int page, T value) {
            Integer update = getUpdates().get(page);
            if (update == null) {
                // This is the first time this transaction updates the page...
                snapshot();
                update = allocator.alloc(1);
                getUpdates().put(page, update);
                getCacheUpdates().put(page, new CacheUpdate(update, value, marshaller));
            } else {
                // We have updated it before...
                switch (update) {
                case PAGE_FREED:
                    throw new PagingException("You should never try to write a page that has been freed.");
                case PAGE_ALLOCATED:
                    getCacheUpdates().put(page, new CacheUpdate(page, value, marshaller));
                    break;
                default:
                    CacheUpdate cu = getCacheUpdates().get(page);
                    if( cu == null ) {
                        throw new PagingException("You should never try to store mix using the cached objects with normal page updates.");
                    }
                    cu.reset(value, marshaller);
                }
            }
        }

        public <T> void remove(EncoderDecoder<T> marshaller, int page) {
            marshaller.remove(ConcurrentTransaction.this, page);
        }
        
        public Allocator allocator() {
            return txallocator;
        }

        public void read(int pageId, Buffer buffer) throws IOPagingException {
           
            Integer updatedPageId = updates == null ? null : updates.get(pageId);
            if (updatedPageId != null) {
                switch (updatedPageId) {
                case PAGE_ALLOCATED:
                case PAGE_FREED:
                    // TODO: Perhaps use a RuntimeException subclass.
                    throw new PagingException("You should never try to read a page that has been allocated or freed.");
                default:
                    // read back in the updated we had done.
                    pageFile.read(updatedPageId, buffer);
                }
            } else {
                // Get the data from the snapshot.
                snapshot().read(pageId, buffer);
            }
        }

        public ByteBuffer slice(SliceType type, int page, int count) throws IOPagingException {
            //TODO: need to improve the design of ranged ops..
            if( type==SliceType.READ ) {
                Integer updatedPageId = updates == null ? null : updates.get(page);
                if (updatedPageId != null) {
                    switch (updatedPageId) {
                    case PAGE_ALLOCATED:
                    case PAGE_FREED:
                        throw new PagingException("You should never try to read a page that has been allocated or freed.");
                    }
                    return pageFile.slice(type, updatedPageId, count);
                } else {
                    // Get the data from the snapshot.
                    return snapshot().slice(page, count);
                }
                
            } else {
                Integer update = getUpdates().get(page);
                if (update == null) {
                    update = allocator.alloc(count);
                    
                    if (type==SliceType.READ_WRITE) {
                        ByteBuffer slice = snapshot().slice(page, count);
                        try {
                            pageFile.write(update, slice);
                        } finally { 
                            pageFile.unslice(slice);
                        }
                    }
                    
                    int end = page+count;
                    for (int i = page; i < end; i++) {
                        getUpdates().put(i, PAGE_ALLOCATED);
                    }
                    getUpdates().put(page, update);
                    
                    return pageFile.slice(type, update, count);
                } else {
                    switch (update) {
                    case PAGE_FREED:
                        throw new PagingException("You should never try to write a page that has been freed.");
                    case PAGE_ALLOCATED:
                        break;
                    default:
                        page = update;
                    }
                }
                return pageFile.slice(type, page, count);
                
            }
            
        }
        
        public void unslice(ByteBuffer buffer) {
            pageFile.unslice(buffer);
        }

        public void write(int page, Buffer buffer) throws IOPagingException {
            Integer update = getUpdates().get(page);
            if (update == null) {
                // We are updating an existing page in the snapshot...
                snapshot();
                update = allocator.alloc(1);
                getUpdates().put(page, update);
                page = update;
            } else {
                switch (update) {
                case PAGE_FREED:
                    throw new PagingException("You should never try to write a page that has been freed.");
                case PAGE_ALLOCATED:
                    break;
                default:
                    page = update;
                }
            }
            pageFile.write(page, buffer);
        }


        public void commit() throws IOPagingException {
            boolean failed = true;
            try {
                if (updates!=null) {
                    Update previousUpdate = snapshot==null ? null : snapshot.updates.get(0);
                    ConcurrentPageFile.this.commit(previousUpdate, updates, cache);
                }
                failed = false;
            } finally {
                // Rollback if the commit fails.
                if (failed) {
                    freeAllocatedPages();
                }
                ConcurrentPageFile.this.releaseSnapshot(snapshot);
                updates = null;
                cache = null;
                snapshot = null;
            }
        }

        public void rollback() throws IOPagingException {
            try {
                if (updates!=null) {
                    freeAllocatedPages();
                }
            } finally {
                ConcurrentPageFile.this.releaseSnapshot(snapshot);
                updates = null;
                cache = null;
                snapshot = null;
            }
        }

        private void freeAllocatedPages() {
            for (Entry<Integer, Integer> entry : updates.entrySet()) {
                switch (entry.getValue()) {
                case PAGE_FREED:
                    // Don't need to do anything..
                    break;
                case PAGE_ALLOCATED:
                default:
                    // We need to free the page that was allocated for the
                    // update..
                    allocator.free(entry.getKey(), 1);
                }
            }
        }

        public Snapshot snapshot() {
            if (snapshot == null) {
                snapshot = aquireSnapshot();
            }
            return snapshot;
        }

        public boolean isReadOnly() {
            return updates == null;
        }

        public HashMap<Integer, CacheUpdate> getCacheUpdates() {
            if( cache==null ) {
                cache = new HashMap<Integer, CacheUpdate>();
            }
            return cache;
        }

        private HashMap<Integer, Integer> getUpdates() {
            if (updates == null) {
                updates = new HashMap<Integer, Integer>();
            }
            return updates;
        }

        public int getPageSize() {
            return pageFile.getPageSize();
        }

        public String toString() { 
            int updatesSize = updates==null ? 0 : updates.size();
            return "{ snapshot: "+this.snapshot+", updates: "+updatesSize+" }";
        }

        public int pages(int length) {
            return pageFile.pages(length);
        }

        public void flush() {
            ConcurrentPageFile.this.flush();
        }

    }

    static class Update extends LinkedNode<Update> implements Serializable {
        private static final long serialVersionUID = 9160865012544031094L;

        transient final AtomicBoolean applied = new AtomicBoolean();
        private final Redo redo;
        
        long base;
        long head;
        Snapshot snapshot;
        
        HashMap<Integer, Integer> updates;
        transient HashMap<Integer, CacheUpdate> cache;

        public Update(long version, HashMap<Integer, Integer> updates, HashMap<Integer, CacheUpdate> cache, Redo redo) {
            this.redo = redo;
            this.head = this.base = version;
            this.updates = updates;
            this.cache = new HashMap<Integer, CacheUpdate>();
        }
        
        /**
         * Merges previous updates that can be merged with this update. 
         */
        public void mergePrevious() {
            prev = getPrevious();
            while( prev!=null && prev.snapshot==null && prev.redo==redo ) {
                
                assert prev.head+1 == this.base;
                this.base = prev.base;
                
                if( prev.updates!=null ) {
                    if(this.updates!=null) {
                        prev.updates.putAll(this.updates);
                    }
                    this.updates = prev.updates;
                }
                
                if( prev.cache!=null ) {
                    if( this.cache!=null ) {
                        prev.cache.putAll(this.cache);
                    }
                    this.cache = prev.cache;
                }
                prev.unlink();
            }
        }

        
        
        public String toString() { 
            int updateSize = updates==null ? 0 : updates.size();
            int cacheSize = cache==null ? 0 : cache.size();
            return "{ base: "+this.base+", head: "+this.head+", updates: "+updateSize+", cache: "+cacheSize+", applied: "+applied+" }";
        }
        
    }

    /**
     * @author chirino
     */
    private class Snapshot {

        private final ArrayList<Update> updates;
        private int references;

        public Snapshot(ArrayList<Update> updates) {
            this.updates = updates;
        }

        private int mapPageId(int page) {
            for (Update update : updates) {
                if( update.updates!=null ) {
                    Integer updatedPage = update.updates.get(page);
                    if (updatedPage != null) {
                        switch (updatedPage) {
                        case PAGE_FREED:
                            throw new PagingException("You should never try to read page that has been freed.");
                        case PAGE_ALLOCATED:
                            break;
                        default:
                            page = updatedPage;
                        }
                        break;
                    }
                }
            }
            return page;
        }

        void read(int pageId, Buffer buffer) throws IOPagingException {
            pageId = mapPageId(pageId);
            pageFile.read(pageId, buffer);
        }

        public ByteBuffer slice(int pageId, int count) {
            pageId = mapPageId(pageId);
            return pageFile.slice(SliceType.READ, pageId, count);
        }
        
        private <T> T cacheLoad(EncoderDecoder<T> marshaller, int pageId) {
            // See if any of the updates in the snapshot have an update of the 
            // requested page...
            for (Update update : updates) {
                if( update.cache!=null ) {
                    CacheUpdate cu = update.cache.get(pageId);
                    if (cu != null) {
                        return cu.<T>value();
                    }
                }
            }
            return readCache.cacheLoad(marshaller, pageId);
        }

        public String toString() { 
            return "{ updates: "+this.updates.size()+", references: "+this.references+" }";
        }
    }

    class ReadCache {
        Map<Integer, Object> map = Collections.synchronizedMap(new LRUCache<Integer, Object>(1024));
        
        @SuppressWarnings("unchecked")
        private <T> T cacheLoad(EncoderDecoder<T> marshaller, int pageId) {
            T rc = (T) map.get(pageId);
            if( rc ==null ) {
                rc = marshaller.load(pageFile, pageId);
                map.put(pageId, rc);
            }
            return rc;
        }        
    }
    
    private final ReadCache readCache = new ReadCache();
    
    /**
     * The redo log is composed of a linked list of RedoBatch records.  A 
     * RedoBatch stores the redo data for multiple updates.
     * 
     * @author chirino
     */
    static private class Redo implements Serializable {
        private static final long serialVersionUID = 1188640492489990493L;
        
        /** the pageId that this redo batch is stored at */
        private transient int page=-1;
        private transient boolean recovered;
        
        private long base;
        private long head;
        
        final HashMap<Integer, Integer> updates = new HashMap<Integer, Integer>();
        transient final HashMap<Integer, CacheUpdate> cache = new HashMap<Integer, CacheUpdate>();
        
        /** points to a previous redo batch page */
        public int previous=-1;

        public String toString() { 
            int count = updates==null ? 0 : updates.size();
            return "{ page: "+this.page+", updates: "+count+", previous: "+previous+" }";
        }

        public int pageCount() {
            int rc = 0;
            if( updates!=null ) {
                rc = updates.size();
            }
            if( cache!=null ) {
                rc = cache.size();
            }
            return rc;
        }
        
    }
    
    private class Header extends Struct {
        public final UTF8String file_magic = new UTF8String(80);
        public final Signed64 base_revision = new Signed64();
        public final Signed32 page_size = new Signed32();
        public final Signed32 free_list_page = new Signed32();
        /** points at the latest redo page which might have been partially stored */ 
        public final Signed32 redo_page = new Signed32();
        /** points at the latest redo page which is guaranteed to be fully stored */
        public final Signed32 synced_redo_page = new Signed32();
        public final Signed64 checksum = new Signed64();
        
        public String toString() { 
            return "{ base_revision: "+this.base_revision.get()+
            ", page_size: "+page_size.get()+", free_list_page: "+free_list_page.get()+
            ", redo_page: "+redo_page.get()+", checksum: "+checksum.get()+
            " }";
        }
    }

    
    /**
     * A list of updates to the page file. The list head points to the most
     * recent update.
     */
//    private final HashMap<Long, Update> updatesMap = new HashMap<Long, Update>();
    private final LinkedNodeList<Update> updatesList = new LinkedNodeList<Update>();
    
    /**
     * This is the next redo that will get logged.  It is currently being built.
     */
    Redo nextRedo;
    
    /**
     * These are stored redos that are waiting for a file sync.  They may or may not survive 
     * a failure.
     */
    private final LinkedList<Redo> unsyncedRedos = new LinkedList<Redo>();
    
    /**
     * These are stored redos that have been file synced.  They should survive a failure.
     */
    private final LinkedList<Redo> syncedRedos = new LinkedList<Redo>();
    
    /**
     * These are updates which have been applied but who's temp pages have not yet been 
     * freed since they are being used by some snapshots.
     */
    private final LinkedList<Update> updatesWaitingCleanup = new LinkedList<Update>();
    
    private final Header header = new Header();

    private final PageFile pageFile;
    private final SimpleAllocator allocator;
    private final MemoryMappedFile file;

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
    
    public ConcurrentPageFile(PageFile pageFile) {
        this.pageFile = pageFile;
        this.file = pageFile.getFile();
        this.allocator = pageFile.allocator();
        ByteBuffer slice = file.slice(false, 0, FILE_HEADER_SIZE);
        this.header.setByteBuffer(slice, slice.position());
        this.updatesList.addLast(new Update(0, null, null, null));
    }

    public Transaction tx() {
        return new ConcurrentTransaction();
    }

    /**
     * Used to initialize a new file or to clear out the 
     * contents of an existing file.
     */
    public void reset() {
        updatesList.clear();
        updatesList.addLast(new Update(0, null, null, null));
        unsyncedRedos.clear();
        updatesWaitingCleanup.clear();
        allocator.clear(); 
        baseRevisionFreePages.clear();
        baseRevisionFreePages.add(0, allocator.getLimit());

        // Initialize the file header..
        this.header.setByteBufferPosition(0);
        this.header.file_magic.set(MAGIC);
        this.header.base_revision.set(0);
        this.header.free_list_page.set(-1);
        this.header.page_size.set(pageFile.getPageSize());
        this.header.redo_page.set(-1);
        replicateHeader();
    }
    
    /**
     * Loads an existing file and replays the redo
     * logs to put it in a consistent state.
     */
    public void recover() {
        
        unsyncedRedos.clear();
        updatesWaitingCleanup.clear();

        this.header.setByteBufferPosition(0);
        long baseRevision = header.base_revision.get();
        updatesList.clear();
        updatesList.addLast(new Update(baseRevision, null, null, null));

        // Initialize the free page list.
        int pageId = header.free_list_page.get();
        if( pageId >= 0 ) {
            baseRevisionFreePages = loadObject(pageId);
            allocator.copy(baseRevisionFreePages);
            Extent.unfree(pageFile, pageId);
        } else {
            allocator.clear(); 
            baseRevisionFreePages.add(0, allocator.getLimit());
        }
        
        // Load the redo batches.
        pageId = header.redo_page.get();
        while( pageId >= 0 ) {
            Redo redo = loadObject(pageId); 
            redo.page = pageId;
            redo.recovered = true;
            Extent.unfree(pageFile, pageId);
            
            pageId=-1;
            if( baseRevision < redo.head ) {
                
                // add first since we are loading redo objects oldest to youngest
                // but want to put them in the list youngest to oldest.
                unsyncedRedos.addFirst(redo);
                
                if( baseRevision < redo.base ) {
                    pageId=redo.previous;
                }
            }
        }
        
        // Apply all the redos..
        applyRedos();
    }


    /**
     * Once this method returns, any previously committed transactions 
     * are flushed and to the disk, ensuring that they will not be lost
     * upon failure. 
     */
    public void flush() {
        
        // Write out the current redo if it has data...
        Redo redo;
        synchronized (this) {
            redo = nextRedo;
            nextRedo = null;
        }
        if( redo == null ) {
            store(redo);
        }

        // Find out if there are unsynced redos...
        redo = null;
        synchronized (this) {
            if( !unsyncedRedos.isEmpty() ) {
                for (Redo r : unsyncedRedos) {
                    syncedRedos.add(r);
                    redo = r;
                }
            }
        }
        
        // Yep.. we had some.. 
        if( redo!=null ) {
            // This is a slow operation..
            file.sync();
            // Update the header so that it knows about the redos that are 
            // guaranteed to survive a failure.
            header().synced_redo_page.set(redo.page);
            replicateHeader();
        }
    }

    private Header header() {
        this.header.getByteBuffer().position(0);
        this.header.setByteBufferPosition(0);
        Header h = this.header;
        return h;
    }
    
    public void store(Redo redo) {
        
        // Write any outstanding deferred cache updates...
        if( redo.cache != null ) {
            for (Entry<Integer, CacheUpdate> entry : redo.cache.entrySet()) {
                CacheUpdate cu = entry.getValue();
                List<Integer> allocatedPages = cu.store(pageFile);
                for (Integer page : allocatedPages) {
                    // add any allocated pages to the update list so that the free 
                    // list gets properly adjusted.
                    redo.updates.put(page, PAGE_ALLOCATED);
                }
            }
        }

        // Link it to the last redo.
        Redo last = unsyncedRedos.getLast();
        if( last!=null ) {
            redo.previous = last.page; 
        }
        
        // Store the redo.
        redo.page = storeObject(redo);
        synchronized (this) {
            unsyncedRedos.add(redo);
        }

        // Update the header to know about the new redo page.
        header().redo_page.set(redo.page);
        replicateHeader();
    }

    /**
     *  Frees up space by applying redos and releasing the pages that
     *  the redo was stored on. 
     */
    public void applyRedos() {

        // We can only apply redos which we know are not partially stored on disk
        // and which hold revisions which are older than the oldest active snapshot.
        ArrayList<Redo> redoList = new ArrayList<Redo>();
        synchronized (this) {
            long snapshotHeadRev = Long.MAX_VALUE;
            Update cur = updatesList.getHead();
            while( cur!=null ) {
                if( cur.snapshot!=null ) {
                    snapshotHeadRev = cur.head;
                    break;
                }
            }
            
            for (Iterator<Redo> i = this.unsyncedRedos.iterator(); i.hasNext();) {
                Redo redo = i.next();
                if (redo.base > snapshotHeadRev) {
                    // we can't apply the rest of the updates, since a snapshot depends on 
                    // the current base revision.
                    // the rest of the updates will have incrementing revision numbers too.
                    break;
                }
                redoList.add(redo);
                i.remove();
            }
        }
        
        // Perhaps we can't do any work...
        if( redoList.isEmpty() ) {
            return;
        }
        
        long baseRevision = header().base_revision.get();

        for (Redo redo : redoList) {
            // revision numbers should be sequentially increasing.
            assert baseRevision+1==redo.base;
            
            for (Entry<Integer, Integer> entry : redo.updates.entrySet()) {
                int key = entry.getKey();
                int value = entry.getValue();
                switch( value ) {
                case PAGE_ALLOCATED:
                    if( redo.recovered ) {
                        allocator.unfree(key, 1);
                    }
                    baseRevisionFreePages.remove(key, 1);
                    break;
                case PAGE_FREED:
                    if( redo.recovered ) {
                        allocator.free(key, 1);
                    }
                    baseRevisionFreePages.add(key, 1);
                    break;
                default:
                    if( redo.recovered ) {
                        allocator.unfree(key, 1);
                    }
                    ByteBuffer slice = pageFile.slice(SliceType.READ, value, 1);
                    try {
                        pageFile.write(key, slice);
                    } finally { 
                        pageFile.unslice(slice);
                    }
                }
            }
            baseRevision = redo.base;
        }

            
        // force to ensure all data is fully stored before the header 
        // starts making reference to new stuff
        file.sync();

        
        Header h = header();
        int previousFreeListPage = h.free_list_page.get();
        h.free_list_page.set(storeObject(baseRevisionFreePages));
        h.base_revision.set(baseRevision);
        replicateHeader();
        
        // Release the previous free list.
        if( previousFreeListPage>=0 ) {
            Extent.free(pageFile, previousFreeListPage);
        }
        
        // Free the space associated with the redo batches
        if( !redoList.isEmpty() ) {
            for (Redo redo : redoList) {
                Extent.free(pageFile, redo.page);
            }
        }
    }

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
    // Transaction calls back to these methods...
    // /////////////////////////////////////////////////////////////////
    synchronized private Snapshot aquireSnapshot() {
        Snapshot snapshot = updatesList.getTail().snapshot;
        if (snapshot == null) {
            ArrayList<Update> updates = updatesList.toArrayListReversed();
            updates.get(0).snapshot = new Snapshot(updates);
        }
        snapshot.references++;
        return snapshot;
    }
    
    synchronized private void releaseSnapshot(Snapshot snapshot) {
        if( snapshot!=null ) {
            synchronized(this) {
                snapshot.references--;
                if( snapshot.references==0 ) {
                    Update update = snapshot.updates.get(0);
                    update.snapshot=null;
                    Update next = update.getNext();
                    if( next !=null ) {
                        next.mergePrevious();
                    }
                }
            }
        }
    }

    /**
     * Attempts to commit a set of page updates.
     * 
     * @param previousUpdate
     * @param pageUpdates
     * @param cache
     */
    private void commit(Update previousUpdate, HashMap<Integer, Integer> pageUpdates, HashMap<Integer, CacheUpdate> cache) {
        
        Redo fullRedo=null;
        synchronized (this) {
            // Perhaps a concurrent update came in before this one...
            long rev;
            if( previousUpdate!=null ) {
                rev = previousUpdate.head;
                Update concurrentUpdate = previousUpdate.getNext();
                while( concurrentUpdate != null ) {
                    // Yep.. there were concurrent updates.  
                    // Make sure we don't don't have update conflict.
                    for (Integer page : pageUpdates.keySet()) {
                        if( concurrentUpdate.updates.containsKey(page) ) {
                            throw new OptimisticUpdateException();
                        }
                    }
        
                    rev = concurrentUpdate.head;
                    concurrentUpdate = concurrentUpdate.getNext();
                }
            } else {
                rev = updatesList.getTail().head;
            }
            rev++;

            if( nextRedo == null ) {
                nextRedo = new Redo();
                nextRedo.base = rev;
            }
            
            nextRedo.head = rev;
            nextRedo.updates.putAll(pageUpdates);
            nextRedo.cache.putAll(cache);
            
            Update value = new Update(rev, pageUpdates, cache, nextRedo);
            updatesList.addLast(value);
            value.mergePrevious();
            
            if( nextRedo.pageCount() > 10 ) {
                fullRedo = nextRedo;
                nextRedo = null;
            }
        }
        if( fullRedo!=null ) {
            store(fullRedo);
        }
    }
    
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
}
