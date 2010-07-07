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

import org.apache.activemq.util.StringSupport;
import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.Allocator;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.OutOfSpaceException;
import org.apache.hawtdb.api.PagingException;
import org.apache.hawtdb.api.Transaction;

import static org.apache.hawtdb.internal.page.Update.*;
import static org.apache.hawtdb.internal.page.DeferredUpdate.*;

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

    private ConcurrentHashMap<Integer, Update> updates;
    private Snapshot snapshot;
    
    private final Allocator txallocator = new Allocator() {
        
        public void free(int pageId, int count) {
            // TODO: this is not a very efficient way to handle allocation ranges.
            int end = pageId+count;
            for (int key = pageId; key < end; key++) {
                Update previous = getUpdates().put(key, update(key).freed() );
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
                getUpdates().put(key, update(key).allocated() );
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
        Update update = updates == null ? null : updates.get(page);
        if( update != null ) {
            DeferredUpdate deferred = update.deferredUpdate();
            if( deferred != null ) {
                return deferred.<T>value();
            } else {
                throw new PagingException("That page was updated with the 'put' method.");
            }
        }
        
        // No?  Then ask the snapshot to load the object.
        T rc = snapshot().getHead().get(marshaller, page);
        if( rc == null ) {
            rc = parent.readCache.cacheLoad(marshaller, page);
        }
        return rc;
    }

    public <T> void put(EncoderDecoder<T> marshaller, int page, T value) {
        ConcurrentHashMap<Integer, Update> updates = getUpdates();
        Update update = updates.get(page);
        if (update == null) {
            // This is the first time this transaction updates the page...
            snapshot();
            updates.put(page, deferred(parent.allocator.alloc(1)).store(value, marshaller).allocated() );
        } else {
            // We have updated it before...
            if( update.wasFreed() ) {
                throw new PagingException("You should never try to update a page that has been freed.");
            }
            
            DeferredUpdate deferred = update.deferredUpdate();
            if( deferred==null ) {
                deferred = deferred(update);
                updates.put(page, deferred);
            }
            deferred.store(value, marshaller);
        }
    }

    public <T> void clear(EncoderDecoder<T> marshaller, int page) {
        ConcurrentHashMap<Integer, Update> updates = getUpdates();
        Update update = updates.get(page);
        
        if( update == null ) {
            updates.put(page, deferred(page).clear(marshaller) );
        } else {
            if( update.wasDeferredStore() ) {
                if( update.wasAllocated() ) {
                    // this transaction created it.. so it should remove it..
                    updates.put(page, update(update));
                } else { 
                    // was an update of a previous location.... 
                    updates.put(page, deferred(page).clear(marshaller));
                }
            } else {
                throw new PagingException("You should never try to clear a page that was not put.");
            }
        }
    }
    
    public Allocator allocator() {
        return txallocator;
    }

    public void read(int page, Buffer buffer) throws IOPagingException {
        // We may need to translate the page due to an update..
        Update update = updates == null ? null : updates.get(page);
        if (update != null) {
            // in this transaction..
            page = update.page();
        } else {
            // in a committed transaction that has not yet been performed.
            page = snapshot().getHead().translatePage(page);  
        }
        parent.pageFile.read(page, buffer);
    }

    public ByteBuffer slice(SliceType type, int page, int count) throws IOPagingException {
        //TODO: wish we could do ranged opps more efficiently.
        
        if( type==SliceType.READ ) {
            Update udpate = updates == null ? null : updates.get(page);
            if (udpate != null) {
                page = udpate.page();
            } else {
                page = snapshot().getHead().translatePage(page);
            }
        } else {
            Update update = getUpdates().get(page);
            if (update == null) {

                // Allocate space of the update redo pages.
                update = update(parent.allocator.alloc(count)).allocated();
                int end = page+count;
                for (int i = page; i < end; i++) {
                    getUpdates().put(i, update(i).allocated());
                }
                
                if (type==SliceType.READ_WRITE) {
                    // Oh he's going to read it too?? then copy the original to the 
                    // redo pages..
                    int originalPage = snapshot().getHead().translatePage(page);
                    ByteBuffer slice = parent.pageFile.slice(SliceType.READ, originalPage, count);
                    try {
                        parent.pageFile.write(update.page, slice);
                    } finally { 
                        parent.pageFile.unslice(slice);
                    }
                }
                
                getUpdates().put(page, update);
            }
            
            // translate the page..
            page = update.page;
        }
        return parent.pageFile.slice(type, page, count);
        
    }
    
    public void unslice(ByteBuffer buffer) {
        parent.pageFile.unslice(buffer);
    }

    public void write(int page, Buffer buffer) throws IOPagingException {
        Update update = getUpdates().get(page);
        if (update == null) {
            // We are updating an existing page in the snapshot...
            snapshot();
            update = update(parent.allocator.alloc(1)).allocated();
            getUpdates().put(page, update);
        }
        parent.pageFile.write(update.page(), buffer);
    }


    public void commit() throws IOPagingException {
        boolean failed = true;
        try {
            if (updates!=null) {
                // If the commit is successful it will release our snapshot..
                parent.commit(snapshot, updates);
            }
            failed = false;
        } finally {
            // Rollback if the commit fails.
            if (failed) {
                // rollback will release our snapshot..
                rollback();
            }
            updates = null;
            snapshot = null;
        }
    }

    public void rollback() throws IOPagingException {
        try {
            if (updates!=null) {
                for (Update update : updates.values()) {
                    if( !update.wasFreed() ) {
                        parent.allocator.free(update.page, 1);
                    }
                }
            }
        } finally {
            if( snapshot!=null ) {
                snapshot.close();
                snapshot = null;
            }
            updates = null;
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
        return "{ \n" +
        	   "  snapshot: "+this.snapshot+", \n"+
        	   "  updates: "+updatesSize+", \n" +
        	   "  parent: "+StringSupport.indent(parent.toString(), 2)+"\n" +
        	   "}";
    }

    public int pages(int length) {
        return parent.pageFile.pages(length);
    }

    public void flush() {
        parent.flush();
    }

}
