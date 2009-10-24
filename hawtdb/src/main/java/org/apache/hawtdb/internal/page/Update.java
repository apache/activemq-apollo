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

import org.apache.hawtdb.api.PagingException;

/**
 * <p>Tracks one page update.
 * </p><p>  
 * To be able to provide snapshot isolation and to make 
 * sure a set of updates can be performed atomically,  updates
 * to existing pages are stored in a 'redo' page.  Once all the updates
 * that are part of the transaction have been verified to be on disk,
 * and no open snapshot would need to access the data on the original page,
 * the contents of the 'redo' page are copied to the original page location
 * and the 'redo' page gets freed.
 * </p><p>
 * A Update object is stored in the updates map in a Commit object.  That map
 * is keyed off the original page location.  The Update page is the location
 * of the 'redo' page.
 * </p><p>
 * Updates to pages which were allocated in the same transaction get done
 * directly against the allocated page since no snapshot would have a view onto 
 * that page.  In this case the update map key would match the update's page.
 * </p><p>
 * An update maintains some bit flags to know if the page was a new allocation
 * or if the update was just freeing a previously allocated page, etc.  This data
 * is used to properly maintain the persisted free page list.
 * </p>
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Update implements Externalizable {
    
    public static final byte PAGE_ALLOCATED   = 0x01 << 0;
    public static final byte PAGE_FREED       = 0x01 << 1;
    public static final byte PAGE_STORE       = 0x01 << 2;
    public static final byte PAGE_CLEAR       = 0x01 << 3;

    private static final long serialVersionUID = -1128410792448869134L;
    
    byte flags;
    int page;

    public Update() {
    }
    
    public Update(Update udpate) {
        this.page = udpate.page;
        this.flags = (byte) (udpate.flags & (PAGE_ALLOCATED|PAGE_FREED));
    }

    public Update(int page) {
        this.page = page;
    }

    public static Update update(Update update) {
        return new Update(update);
    }
    public static Update update(int page) {
        return new Update(page);
    }
    
    public int page() {
        if( wasFreed() ) {
            throw new PagingException("You should never try to read or write page that has been freed.");
        }
        return page;
    }

    public DeferredUpdate deferredUpdate() {
        return null;
    }
    
    public Update allocated() {
        flags = (byte) ((flags & ~PAGE_FREED) | PAGE_ALLOCATED);
        return this;
    }
    
    public Update freed() {
        flags = (byte) ((flags & ~PAGE_ALLOCATED) | PAGE_FREED);
        return this;
    }

    public boolean wasFreed() {
        return (flags & PAGE_FREED)!=0 ;
    }
    public boolean wasAllocated() {
        return (flags & PAGE_ALLOCATED)!=0;
    }
    public boolean wasDeferredStore() {
        return (flags & PAGE_STORE)!=0 ;
    }
    public boolean wasDeferredClear() {
        return (flags & PAGE_CLEAR)!=0;
    }
    
    @Override
    public String toString() {
        return "{ page: "+page+", flags: "+flags+" }";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        page = in.readInt();
        flags = in.readByte();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(page);
        out.writeByte(flags);
    }
    
}