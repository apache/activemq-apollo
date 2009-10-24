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
import java.util.Map;

import org.apache.activemq.util.buffer.Buffer;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.Paged.SliceType;

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
    /**
     * 
     */
    private final HawtPageFile hawtPageFile;
    final Batch parent;
    
    public SnapshotHead(HawtPageFile hawtPageFile, Batch parent) {
        this.hawtPageFile = hawtPageFile;
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
        this.hawtPageFile.pageFile.read(pageId, buffer);
    }

    public ByteBuffer slice(int pageId, int count) {
        pageId = mapPageId(pageId);
        return this.hawtPageFile.pageFile.slice(SliceType.READ, pageId, count);
    }
    
    public void open(Batch base) {
        references++;
        while( true ) {
            base.references++;
            if(base == parent ) {
                break;
            }
            base = base.getNext();
        }
    }
    
    public void close(Batch base) {
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
        Batch curRedo = parent;
        BatchEntry curEntry = getPrevious();
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
        Batch curRedo = parent;
        BatchEntry curEntry = getPrevious();
        while( true ) {
            if( curRedo.isPerformed() ) {
                break;
            }
            
            while( curEntry!=null ) {
                Commit commit = curEntry.isCommit();
                if( commit !=null ) {
                    Update update = commit.updates.get(page);
                    if( update!=null ) {
                        DeferredUpdate du  = update.deferredUpdate();
                        if (du!=null) {
                            return du.<T>value();
                        }
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
        return this.hawtPageFile.readCache.cacheLoad(marshaller, page);
    }
    
    public long commitCheck(Map<Integer, Update> pageUpdates) {
        long rc=0;
        Batch curRedo = parent;
        BatchEntry curEntry = getNext();
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