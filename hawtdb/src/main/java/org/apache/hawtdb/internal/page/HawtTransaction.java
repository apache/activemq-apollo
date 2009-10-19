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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.activemq.util.buffer.Buffer;
import org.apache.hawtdb.api.Allocator;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.OutOfSpaceException;
import org.apache.hawtdb.api.PagingException;
import org.apache.hawtdb.api.Transaction;
import org.apache.hawtdb.internal.page.HawtPageFile.DeferredUpdate;
import org.apache.hawtdb.internal.page.HawtPageFile.Snapshot;

/**
 * Transaction objects are NOT thread safe. Users of this object should
 * guard it from concurrent access.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class HawtTransaction implements Transaction {
    /**
     * 
     */
    private final HawtPageFile parent;

    /**
     * @param concurrentPageFile
     */
    HawtTransaction(HawtPageFile concurrentPageFile) {
        parent = concurrentPageFile;
    }

    private HashMap<Integer, DeferredUpdate> deferredUpdates;
    private HashMap<Integer, Integer> updates;
    private Snapshot snapshot;
    
    private final Allocator txallocator = new Allocator() {
        
        public void free(int pageId, int count) {
            // TODO: this is not a very efficient way to handle allocation ranges.
            int end = pageId+count;
            for (int key = pageId; key < end; key++) {
                Integer previous = getUpdates().put(key, HawtPageFile.PAGE_FREED);
                
                // If it was an allocation that was done in this
                // tx, then we can directly release it.
                assert previous!=null;
                if( previous == HawtPageFile.PAGE_ALLOCATED) {
                    getUpdates().remove(key);
                    HawtTransaction.this.parent.allocator.free(key, 1);
                }
            }
        }
        
        public int alloc(int count) throws OutOfSpaceException {
            int pageId = HawtTransaction.this.parent.allocator.alloc(count);
            // TODO: this is not a very efficient way to handle allocation ranges.
            int end = pageId+count;
            for (int key = pageId; key < end; key++) {
                getUpdates().put(key, HawtPageFile.PAGE_ALLOCATED);
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
            return HawtTransaction.this.parent.allocator.getLimit();
        }

        public boolean isAllocated(int page) {
            return HawtTransaction.this.parent.allocator.isAllocated(page);
        }

    };

    public <T> T get(EncoderDecoder<T> marshaller, int page) {
        // Perhaps the page was updated in the current transaction...
        DeferredUpdate rc = deferredUpdates == null ? null : deferredUpdates.get(page);
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
            update = parent.allocator.alloc(1);
            getUpdates().put(page, update);
            getCacheUpdates().put(page, new HawtPageFile.DeferredUpdate(update, value, marshaller));
        } else {
            // We have updated it before...
            switch (update) {
            case HawtPageFile.PAGE_FREED:
                throw new PagingException("You should never try to write a page that has been freed.");
            case HawtPageFile.PAGE_ALLOCATED:
                getCacheUpdates().put(page, new HawtPageFile.DeferredUpdate(page, value, marshaller));
                break;
            default:
                DeferredUpdate cu = getCacheUpdates().get(page);
                if( cu == null ) {
                    throw new PagingException("You should never try to store mix using the cached objects with normal page updates.");
                }
                cu.reset(value, marshaller);
            }
        }
    }

    public <T> void remove(EncoderDecoder<T> marshaller, int page) {
        marshaller.remove(this, page);
    }
    
    public Allocator allocator() {
        return txallocator;
    }

    public void read(int pageId, Buffer buffer) throws IOPagingException {
       
        Integer updatedPageId = updates == null ? null : updates.get(pageId);
        if (updatedPageId != null) {
            switch (updatedPageId) {
            case HawtPageFile.PAGE_ALLOCATED:
            case HawtPageFile.PAGE_FREED:
                // TODO: Perhaps use a RuntimeException subclass.
                throw new PagingException("You should never try to read a page that has been allocated or freed.");
            default:
                // read back in the updated we had done.
                parent.pageFile.read(updatedPageId, buffer);
            }
        } else {
            // Get the data from the snapshot.
            snapshot().read(pageId, buffer);
        }
    }

    public ByteBuffer slice(SliceType type, int page, int count) throws IOPagingException {
        //TODO: need to improve the design of ranged ops..
        if( type==SliceType.READ ) {
            Integer udpate = updates == null ? null : updates.get(page);
            if (udpate != null) {
                switch (udpate) {
                case HawtPageFile.PAGE_FREED:
                    throw new PagingException("You should never try to read a page that has been allocated or freed.");
                case HawtPageFile.PAGE_ALLOCATED:
                    break;
                default:
                    page = udpate;
                }
                return parent.pageFile.slice(type, page, count);
            } else {
                // Get the data from the snapshot.
                return snapshot().slice(page, count);
            }
            
        } else {
            Integer update = getUpdates().get(page);
            if (update == null) {
                update = parent.allocator.alloc(count);
                
                if (type==SliceType.READ_WRITE) {
                    ByteBuffer slice = snapshot().slice(page, count);
                    try {
                        parent.pageFile.write(update, slice);
                    } finally { 
                        parent.pageFile.unslice(slice);
                    }
                }
                
                int end = page+count;
                for (int i = page; i < end; i++) {
                    getUpdates().put(i, HawtPageFile.PAGE_ALLOCATED);
                }
                getUpdates().put(page, update);
                
                return parent.pageFile.slice(type, update, count);
            } else {
                switch (update) {
                case HawtPageFile.PAGE_FREED:
                    throw new PagingException("You should never try to write a page that has been freed.");
                case HawtPageFile.PAGE_ALLOCATED:
                    break;
                default:
                    page = update;
                }
            }
            return parent.pageFile.slice(type, page, count);
            
        }
        
    }
    
    public void unslice(ByteBuffer buffer) {
        parent.pageFile.unslice(buffer);
    }

    public void write(int page, Buffer buffer) throws IOPagingException {
        Integer update = getUpdates().get(page);
        if (update == null) {
            // We are updating an existing page in the snapshot...
            snapshot();
            update = parent.allocator.alloc(1);
            getUpdates().put(page, update);
            page = update;
        } else {
            switch (update) {
            case HawtPageFile.PAGE_FREED:
                throw new PagingException("You should never try to write a page that has been freed.");
            case HawtPageFile.PAGE_ALLOCATED:
                break;
            default:
                page = update;
            }
        }
        parent.pageFile.write(page, buffer);
    }


    public void commit() throws IOPagingException {
        boolean failed = true;
        try {
            if (updates!=null) {
                parent.commit(snapshot, updates, deferredUpdates);
            }
            failed = false;
        } finally {
            // Rollback if the commit fails.
            if (failed) {
                freeAllocatedPages();
            }
            parent.closeSnapshot(snapshot);
            updates = null;
            deferredUpdates = null;
            snapshot = null;
        }
    }

    public void rollback() throws IOPagingException {
        try {
            if (updates!=null) {
                freeAllocatedPages();
            }
        } finally {
            parent.closeSnapshot(snapshot);
            updates = null;
            deferredUpdates = null;
            snapshot = null;
        }
    }

    private void freeAllocatedPages() {
        for (Entry<Integer, Integer> entry : updates.entrySet()) {
            switch (entry.getValue()) {
            case HawtPageFile.PAGE_FREED:
                // Don't need to do anything..
                break;
            case HawtPageFile.PAGE_ALLOCATED:
            default:
                // We need to free the page that was allocated for the
                // update..
                parent.allocator.free(entry.getKey(), 1);
            }
        }
    }

    public Snapshot snapshot() {
        if (snapshot == null) {
            snapshot = parent.openSnapshot();
        }
        return snapshot;
    }

    public boolean isReadOnly() {
        return updates == null;
    }

    public HashMap<Integer, DeferredUpdate> getCacheUpdates() {
        if( deferredUpdates==null ) {
            deferredUpdates = new HashMap<Integer, DeferredUpdate>();
        }
        return deferredUpdates;
    }

    private HashMap<Integer, Integer> getUpdates() {
        if (updates == null) {
            updates = new HashMap<Integer, Integer>();
        }
        return updates;
    }

    public int getPageSize() {
        return parent.pageFile.getPageSize();
    }

    public String toString() { 
        int updatesSize = updates==null ? 0 : updates.size();
        return "{ snapshot: "+this.snapshot+", updates: "+updatesSize+" }";
    }

    public int pages(int length) {
        return parent.pageFile.pages(length);
    }

    public void flush() {
        parent.flush();
    }

}