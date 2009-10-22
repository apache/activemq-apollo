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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.util.buffer.Buffer;
import org.apache.hawtdb.api.Allocator;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.OutOfSpaceException;
import org.apache.hawtdb.api.PagingException;
import org.apache.hawtdb.api.Transaction;
import org.apache.hawtdb.internal.page.HawtPageFile.DeferredUpdate;
import org.apache.hawtdb.internal.page.HawtPageFile.Snapshot;
import org.apache.hawtdb.internal.page.HawtPageFile.Update;

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

    private ConcurrentHashMap<Integer, DeferredUpdate> deferredUpdates;
    private ConcurrentHashMap<Integer, Update> updates;
    private Snapshot snapshot;
    
    private final Allocator txallocator = new Allocator() {
        
        public void free(int pageId, int count) {
            // TODO: this is not a very efficient way to handle allocation ranges.
            int end = pageId+count;
            for (int key = pageId; key < end; key++) {
                Update previous = getUpdates().put(key, Update.freed(key));
                if( previous!=null && previous.wasAllocated() ) {
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
                getUpdates().put(key, Update.allocated(key));
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
        return snapshot().head.cacheLoad(marshaller, page);
    }

    public <T> void put(EncoderDecoder<T> marshaller, int page, T value) {
        Update update = getUpdates().get(page);
        if (update == null) {
            // This is the first time this transaction updates the page...
            snapshot();
            update = Update.allocated(parent.allocator.alloc(1));
            getUpdates().put(page, update);
            getCacheUpdates().put(page, new HawtPageFile.DeferredUpdate(update.update_location, value, marshaller));
        } else {
            // We have updated it before...
            if( update.wasFreed() ) {
                throw new PagingException("You should never try to write a page that has been freed.");
            }
            if( update.wasAllocated() ) {
                getCacheUpdates().put(page, new HawtPageFile.DeferredUpdate(page, value, marshaller));
            } else {
                DeferredUpdate cu = getCacheUpdates().get(page);
                if( cu == null ) {
                    throw new PagingException("You should never try to store mix using the cached objects with normal page updates.");
                }
                cu.reset(value, marshaller);
            }
        }
    }

    public <T> void remove(EncoderDecoder<T> marshaller, int page) {
        DeferredUpdate deferredUpdate = getCacheUpdates().remove(page);
        if( deferredUpdate==null ) {
            // add a deferred update to remove the value.
            getCacheUpdates().put(page, new DeferredUpdate(page, null, marshaller));
        } else {
            if( deferredUpdate.value == null ) {
                // undo.. user error.
                getCacheUpdates().put(deferredUpdate.page, deferredUpdate);
                throw new PagingException("You should never try to remove a page that has been removed.");
            }
        }
    }
    
    public Allocator allocator() {
        return txallocator;
    }

    public void read(int pageId, Buffer buffer) throws IOPagingException {
        Update update = updates == null ? null : updates.get(pageId);
        if (update != null) {
            parent.pageFile.read(update.page(), buffer);
        } else {
            // Get the data from the snapshot.
            snapshot().head.read(pageId, buffer);
        }
    }

    public ByteBuffer slice(SliceType type, int page, int count) throws IOPagingException {
        //TODO: need to improve the design of ranged ops..
        if( type==SliceType.READ ) {
            Update udpate = updates == null ? null : updates.get(page);
            if (udpate != null) {
                return parent.pageFile.slice(type, udpate.page(), count);
            } else {
                // Get the data from the snapshot.
                return snapshot().head.slice(page, count);
            }
            
        } else {
            Update update = getUpdates().get(page);
            if (update == null) {
                update = Update.allocated(parent.allocator.alloc(count));
                if (type==SliceType.READ_WRITE) {
                    ByteBuffer slice = snapshot().head.slice(page, count);
                    try {
                        parent.pageFile.write(update.update_location, slice);
                    } finally { 
                        parent.pageFile.unslice(slice);
                    }
                }
                
                int end = page+count;
                for (int i = page; i < end; i++) {
                    getUpdates().put(i, Update.allocated(i));
                }
                getUpdates().put(page, update);
                
                return parent.pageFile.slice(type, update.update_location, count);
            }
            return parent.pageFile.slice(type, update.page(), count);
        }
        
    }
    
    public void unslice(ByteBuffer buffer) {
        parent.pageFile.unslice(buffer);
    }

    public void write(int page, Buffer buffer) throws IOPagingException {
        Update update = getUpdates().get(page);
        if (update == null) {
            // We are updating an existing page in the snapshot...
            snapshot();
            update = Update.allocated(parent.allocator.alloc(1));
            getUpdates().put(page, update);
        }
        parent.pageFile.write(update.page(), buffer);
    }


    public void commit() throws IOPagingException {
        boolean failed = true;
        try {
            if (updates!=null) {
                // If the commit is successful it will release our snapshot..
                parent.commit(snapshot, updates, deferredUpdates);
            }
            failed = false;
        } finally {
            // Rollback if the commit fails.
            if (failed) {
                // rollback will release our snapshot..
                rollback();
            }
            updates = null;
            deferredUpdates = null;
            snapshot = null;
        }
    }

    public void rollback() throws IOPagingException {
        try {
            if (updates!=null) {
                for (Update update : updates.values()) {
                    if( !update.wasFreed() ) {
                        parent.allocator.free(update.update_location, 1);
                    }
                }
            }
        } finally {
            if( snapshot!=null ) {
                snapshot.close();
                snapshot = null;
            }
            updates = null;
            deferredUpdates = null;
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

    public ConcurrentHashMap<Integer, DeferredUpdate> getCacheUpdates() {
        if( deferredUpdates==null ) {
            deferredUpdates = new ConcurrentHashMap<Integer, DeferredUpdate>();
        }
        return deferredUpdates;
    }

    private ConcurrentHashMap<Integer, Update> getUpdates() {
        if (updates == null) {
            updates = new ConcurrentHashMap<Integer, Update>();
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