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
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hawtdb.api.Allocator;
import org.apache.hawtdb.api.OptimisticUpdateException;

/**
 * Tracks the updates that were part of a transaction commit.
 * Multiple commit objects can be merged into a single commit.
 * 
 * A Commit is a BatchEntry and stored in Batch object.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class Commit extends BatchEntry implements Externalizable {

    /** oldest revision in the commit range. */
    private long base; 
    /** newest revision in the commit range, will match base if this only tracks one commit */ 
    private long head;
    
    /** all the page updates that are part of the redo */
    ConcurrentHashMap<Integer, Update> updates;


    public Commit() {
    }
    
    public Commit(long version, ConcurrentHashMap<Integer, Update> updates) {
        this.head = this.base = version;
        this.updates = updates;
    }
    
    
    @Override
    public long getHeadRevision() {
        return head;
    }
    
    @Override
    public Commit isCommit() {
        return this;
    }

    
    public String toString() { 
        int updateSize = updates==null ? 0 : updates.size();
        return "{ base: "+this.base+", head: "+this.head+", updates: "+updateSize+" }";
    }

    public long commitCheck(Map<Integer, Update> newUpdate) {
        for (Integer page : newUpdate.keySet()) {
            if( updates.containsKey( page ) ) {
                throw new OptimisticUpdateException();
            }
        }
        return head;
    }

    public void merge(Allocator allocator, long rev, ConcurrentHashMap<Integer, Update> updates) {
        assert head+1 == rev;
        head=rev;
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
    void merge(Allocator allocator, int page, Update update) {
        Update previous = this.updates.put(page, update);
        if (previous != null) {
            
            if( update.wasFreed() ) {
                // we can undo the previous update
                if( previous.page != page ) {
                    allocator.free(previous.page, 1);
                }
                if( previous.wasAllocated() ) {
                    allocator.free(page, 1);
                }
                this.updates.remove(page);
                
                // No other merging is needed now..
                return;
            }
            
            // we are undoing the previous update /w this new update.
            if( previous.page != page ) {
                allocator.free(previous.page, 1);
            }
            
            // we may be updating a previously allocated page,
            // if so we need to mark the new page as allocated too.
            if( previous.wasAllocated() ) {
                update.allocated();
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