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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;
import org.apache.hawtdb.api.Paged;

/**
 * Aggregates a group of commits so that they can be more efficiently
 * stored to disk.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class Batch extends LinkedNode<Batch> implements Externalizable, Iterable<Commit> {
    private static final long serialVersionUID = 1188640492489990493L;
    
    /** the pageId that this redo batch is stored at */
    int page=-1;
    /** points to a previous redo batch page */
    public int previous=-1;
    /** was the redo loaded in the {@link recover} method */
    boolean recovered;
    
    /** the commits and snapshots in the redo */ 
    final LinkedNodeList<BatchEntry> entries = new LinkedNodeList<BatchEntry>();
    /** tracks how many snapshots are referencing the redo */
    int snapshots;
    /** the oldest commit in this redo */
    public long base=-1;
    /** the newest commit in this redo */
    public long head;

    boolean performed;
    
    public Batch() {
    }
    
    public boolean isPerformed() {
        return performed;
    }

    public Batch(long head) {
        this.head = head;
    }

    public String toString() { 
        return "{ page: "+this.page+", base: "+base+", head: "+head+", references: "+snapshots+", entries: "+entries.size()+" }";
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


    private Commit nextCommit(BatchEntry entry) {
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
            if( commit.updates != null ) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                    DeferredUpdate du = entry.getValue().deferredUpdate();
                    if( du == null ) {
                        continue;
                    }
                    if( du.wasDeferredClear() ) {
                        List<Integer> freePages = du.marshaller.remove(pageFile, du.page);
                        for (Integer page : freePages) {
                            commit.merge(pageFile.allocator(), page, Update.update(page).freed());
                        }
                    } else if( du.wasDeferredStore() ) {
                        List<Integer> allocatedPages = du.store(pageFile);
                        for (Integer page : allocatedPages) {
                            // add any allocated pages to the update list so that the free 
                            // list gets properly adjusted.
                            commit.merge(pageFile.allocator(), page, Update.update(page).allocated());
                        }
                    }
                }
            }
        }
    }

    public void release(SimpleAllocator allocator) {
        for (Commit commit : this) {
            for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                int key = entry.getKey();
                Update value = entry.getValue();
                if( value.wasFreed() ) {
                    allocator.free(key, 1);
                } else if( key != value.page ) {
                    // need to free the udpate page..
                    allocator.free(value.page, 1);
                }
            }
        }
    }

}