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

import java.io.ObjectStreamException;
import java.util.List;

import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.Paged;

/**
 * A deferred update is an update which has not yet been performed, but 
 * which holds onto all the info needed to do the update.
 * 
 * Encoding java objects to do page updates can be CPU intensive, and if 
 * the same pages are getting updated frequently then deferring updates
 * will save encoding passes sine older updates may get discarded due 
 * to a more more recent update of the same page.  
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DeferredUpdate extends Update {
    EncoderDecoder<?> marshaller;
    Object value;

    public DeferredUpdate(Update update) {
        super(update);
    }
    public DeferredUpdate(int page) {
        super(page);
    }
    
    public static DeferredUpdate deferred(int page) {
        return new DeferredUpdate(page);
    }
    
    public static DeferredUpdate deferred(Update update) {
        return new DeferredUpdate(update);
    }

    public DeferredUpdate deferredUpdate() {
        return this;
    }
    
    public DeferredUpdate store(Object value, EncoderDecoder<?> marshaller) {
        this.value = value;
        this.marshaller = marshaller;
        flags = (byte) ((flags & ~PAGE_CLEAR) | PAGE_STORE);
        return this;
    }

    public DeferredUpdate clear(EncoderDecoder<?> marshaller) {
        this.marshaller= marshaller;
        this.value=null;
        flags = (byte) ((flags & ~PAGE_STORE) | PAGE_CLEAR);
        return this;
    }
    
    @SuppressWarnings("unchecked")
    <T> T value() {
        return (T) value;
    }
    
    @SuppressWarnings("unchecked")
    public List<Integer> store(Paged paged) {
        return ((EncoderDecoder)marshaller).store(paged, page, value);
    }

    public Object writeReplace() throws ObjectStreamException {
        return new Update(this);
    }
    
}