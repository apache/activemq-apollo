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

import java.util.Map;

import org.apache.hawtdb.api.EncoderDecoder;

/**
 * 
 * A SnapshotHead is BatchEntry and stored in a Batch, in the same
 * list as the Commit objects.  It's main purpose is to separate 
 * commits so that Commits after snapshot do not get merged with commits
 * before the snapshot.
 * 
 * A SnapshotHead allows transactions to get a point in time view of the 
 * page file.
 *  
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class SnapshotHead extends BatchEntry {

    final Batch parent;
    final long headRevision;
    
    public SnapshotHead(Batch parent) {
        this.parent = parent;
        BatchEntry lastEntry = this.parent.entries.getTail();
        this.headRevision = (lastEntry == null ? this.parent.head : lastEntry.getHeadRevision())+1;
    }

    /** The number of times this snapshot has been opened. */
    protected int snapshots;
    
    public String toString() { 
        return "{ references: "+this.snapshots+" }";
    }

    @Override
    public long getHeadRevision() {
        return headRevision;
    }

    public SnapshotHead isSnapshotHead() {
        return this;
    }
    
    public int translatePage(int page) {
        // Look for the page in the previous commits..
        Batch batch = parent;
        BatchEntry tail = null;
        BatchEntry entry = this;
        while( true ) {
            if( batch.isPerformed() ) {
                break;
            }
            
            while( true ) {
                if( tail == null ) {
                    tail = entry;
                } else if( !(entry.getHeadRevision() < tail.getHeadRevision()) ) {
                    break;
                }
                
                Commit commit = entry.isCommit();
                if( commit !=null ) {
                    Update update = commit.updates.get(page);
                    if( update!=null ) {
                        return update.page();
                    }
                }
                entry = entry.getPreviousCircular();
            }
            
            batch = batch.getPrevious();
            if( batch==null ) {
                break;
            }
            tail = null;
            entry = batch.entries.getTail();
        }
        return page;
    }
    
    
    public <T> T get(EncoderDecoder<T> marshaller, int page) {
        Batch batch = parent;
        BatchEntry tail = null;
        BatchEntry entry = this;
        
        while( true ) {
            if( batch.isPerformed() ) {
                break;
            }
            
            while( true ) {
                if( tail == null ) {
                    tail = entry;
                } else if( !(entry.getHeadRevision() < tail.getHeadRevision()) ) {
                    break;
                }
                
                Commit commit = entry.isCommit();
                if( commit !=null ) {
                    Update update = commit.updates.get(page);
                    if( update!=null ) {
                        DeferredUpdate du  = update.deferredUpdate();
                        if (du!=null) {
                            return du.<T>value();
                        }
                    }
                }
                entry = entry.getPreviousCircular();
            }
            
            batch = batch.getPrevious();
            if( batch==null ) {
                break;
            }
            tail = null;
            entry = batch.entries.getTail();
        }
        return null;
    }
    
    public long commitCheck(Map<Integer, Update> pageUpdates) {
        long rc=parent.head;
        Batch batch = parent;
        BatchEntry entry = getNext();
        while( true ) {
            while( entry!=null ) {
                Commit commit = entry.isCommit();
                if( commit!=null ) {
                    rc = commit.commitCheck(pageUpdates);
                }
                entry = entry.getNext();
            }
            
            batch = batch.getNext();
            if( batch==null ) {
                break;
            }
            entry = batch.entries.getHead();
        }
        return rc;
    }
            
}