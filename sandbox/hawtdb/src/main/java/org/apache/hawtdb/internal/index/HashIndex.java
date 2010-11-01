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
package org.apache.hawtdb.internal.index;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hawtdb.api.BTreeIndexFactory;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.HashIndexFactory;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.IndexException;
import org.apache.hawtdb.api.IndexVisitor;
import org.apache.hawtdb.api.Paged;


/**
 * Hash Index implementation.  The hash buckets store entries in a b+tree.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndex<Key,Value> implements Index<Key,Value> {
    
    private static final Logger LOG = LoggerFactory.getLogger(HashIndex.class);
    private final BTreeIndexFactory<Key, Value> BIN_FACTORY = new BTreeIndexFactory<Key, Value>();
    
    private final Paged paged;
    private final int page;
    private final int maximumBucketCapacity;
    private final int minimumBucketCapacity;
    private final boolean fixedCapacity;
    private final int loadFactor;
    private final int initialBucketCapacity;
    private final boolean deferredEncoding;

    private Buckets<Key,Value> buckets;

    public HashIndex(Paged paged, int page, HashIndexFactory<Key,Value> factory) {
        this.paged = paged;
        this.page = page;
        this.maximumBucketCapacity = factory.getMaximumBucketCapacity();
        this.minimumBucketCapacity = factory.getMinimumBucketCapacity();
        this.loadFactor = factory.getLoadFactor();
        this.deferredEncoding = factory.isDeferredEncoding();
        this.initialBucketCapacity = factory.getBucketCapacity();
        this.BIN_FACTORY.setKeyMarshaller(factory.getKeyMarshaller());
        this.BIN_FACTORY.setValueMarshaller(factory.getValueMarshaller());
        this.BIN_FACTORY.setDeferredEncoding(this.deferredEncoding);
        this.fixedCapacity = this.minimumBucketCapacity==this.maximumBucketCapacity && this.maximumBucketCapacity==this.initialBucketCapacity;
    }

    public HashIndex<Key, Value> create() {
        buckets = new Buckets<Key, Value>(this);
        buckets.create(initialBucketCapacity);
        storeBuckets();
        return this;
    }

    public HashIndex<Key, Value> open() {
        loadBuckets();
        return this;
    }


    public Value get(Key key) {
        return buckets.bucket(key).get(key);
    }
    
    public boolean containsKey(Key key) {
        return buckets.bucket(key).containsKey(key);
    }
    
    public Value put(Key key, Value value) {
        
        Index<Key, Value> bucket = buckets.bucket(key);
        
        if( fixedCapacity ) {
            return bucket.put(key,value);
        }
        
        boolean wasEmpty = bucket.isEmpty();
        Value put = bucket.put(key,value);

        if (wasEmpty) {
            buckets.active++;
            storeBuckets();
        }
        
        if (buckets.active >= buckets.increaseThreshold) {
            int capacity = Math.min(this.maximumBucketCapacity, buckets.capacity * 4);
            if (buckets.capacity != capacity) {
                this.changeCapacity(capacity);
            }
        }
        return put;
    }
    
    public Value remove(Key key) {
        Index<Key, Value> bucket = buckets.bucket(key);
        
        if( fixedCapacity ) {
            return bucket.remove(key);
        }
        
        boolean wasEmpty = bucket.isEmpty();
        Value rc = bucket.remove(key);
        boolean isEmpty = bucket.isEmpty();
        
        if (!wasEmpty && isEmpty) {
            buckets.active--;
            storeBuckets();
        }

        if (buckets.active <= buckets.decreaseThreshold) {
            int capacity = Math.max(minimumBucketCapacity, buckets.capacity / 2);
            if (buckets.capacity != capacity) {
                changeCapacity(capacity);
            }
        }
        return rc;
    }

    public void clear() {
        buckets.clear();
        if (buckets.capacity!=initialBucketCapacity) {
            changeCapacity(initialBucketCapacity);
        }
    }
    
    public Iterator<Entry<Key, Value>> iterator() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(IndexVisitor<Key, Value> visitor) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    public int size() {
        int rc=0;
        for (int i = 0; i < buckets.capacity; i++) {
            rc += buckets.bucket(i).size();
        }
        return rc;
    }
    
    public boolean isEmpty() {
        return buckets.active==0;
    }    
    
    public void destroy() {
        buckets.destroy();
        buckets = null;
    }
    public int getPage() {
        return page;
    }

    // /////////////////////////////////////////////////////////////////
    // Helper methods Methods
    // /////////////////////////////////////////////////////////////////
    private void changeCapacity(final int capacity) {
        LOG.debug("Resizing to: "+capacity);
        
        Buckets<Key, Value> next = new Buckets<Key, Value>(this);
        next.create(capacity);

        // Copy the data from the old buckets to the new buckets.
        for (int i = 0; i < buckets.capacity; i++) {
            Index<Key, Value> bin = buckets.bucket(i);
            HashSet<Integer> activeBuckets = new HashSet<Integer>();
            for (Map.Entry<Key, Value> entry : bin) {
                Key key = entry.getKey();
                Value value = entry.getValue();
                Index<Key, Value> bucket = next.bucket(key);
                bucket.put(key, value);
                if( activeBuckets.add(bucket.getPage()) ) {
                    next.active++;
                }
            }
        }
        
        buckets.destroy();
        buckets = next;
        storeBuckets();
        
        LOG.debug("Resizing done.  New bins start at: "+buckets.bucketsPage);        
    }

    public String toString() {
        return "{ page: "+page+", buckets: "+buckets+" }";
    }
    
    private void storeBuckets() {
        if( deferredEncoding ) {
            paged.put(BUCKET_ENCODER_DECODER, page, buckets);
        } else {
            BUCKET_ENCODER_DECODER.store(paged, page, buckets);
        }
    }
    
    private void loadBuckets() {
        if( deferredEncoding ) {
            buckets = paged.get(BUCKET_ENCODER_DECODER, page);
        } else {
            buckets = BUCKET_ENCODER_DECODER.load(paged, page);
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // Helper classes
    // /////////////////////////////////////////////////////////////////

    /** 
     * This is the data stored in the index header.  It knows where
     * the hash buckets are stored at an keeps usage statistics about
     * those buckets. 
     */
    static private class Buckets<Key,Value> {

        final HashIndex<Key,Value> index;
        int bucketsPage=-1;
        int active;
        int capacity;
        
        int increaseThreshold;
        int decreaseThreshold;

        public Buckets(HashIndex<Key, Value> index) {
            this.index = index;
        }

        private void calcThresholds() {
            increaseThreshold = (capacity * index.loadFactor)/100;
            decreaseThreshold = (capacity * index.loadFactor * index.loadFactor ) / 20000;
        }

        void create(int capacity) {
            this.active = 0;
            this.capacity = capacity;
            this.bucketsPage = index.paged.allocator().alloc(capacity);
            for (int i = 0; i < capacity; i++) {
                index.BIN_FACTORY.create(index.paged, (bucketsPage + i));
            }
            calcThresholds();
        }
        
        public void destroy() {
            clear();
            index.paged.allocator().free(bucketsPage, capacity);
        }
        
        public void clear() {
            for (int i = 0; i < index.buckets.capacity; i++) {
                index.buckets.bucket(i).clear();
            }
            index.buckets.active = 0;
            index.buckets.calcThresholds();
        }
        
        Index<Key,Value> bucket(int bucket) {
            return index.BIN_FACTORY.open(index.paged, bucketsPage+bucket);
        }

        Index<Key,Value> bucket(Key key) {
            int i = index(key);
            return index.BIN_FACTORY.open(index.paged, bucketsPage+i);
        }

        int index(Key x) {
            return Math.abs(x.hashCode()%capacity);
        }
        
        @Override
        public String toString() {
            return "{ page:"+bucketsPage+", capacity: "+capacity+", active: "+active+", increase threshold: "+increaseThreshold+", decrease threshold: "+decreaseThreshold+" }";
        }
        
    }

    public static final Buffer MAGIC = new Buffer(new byte[] {'h', 'a', 's', 'h'});
    public static final int HEADER_SIZE = MAGIC.length+ 12; // bucketsPage, capacity, active

    private final EncoderDecoder<Buckets<Key, Value>> BUCKET_ENCODER_DECODER = new EncoderDecoder<Buckets<Key, Value>>() {
        @Override
        public List<Integer> store(Paged paged, int page, Buckets<Key, Value> buckets) {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(HEADER_SIZE);
            try {
                Buffer magic2 = MAGIC;
                os.write(magic2.data, MAGIC.offset, MAGIC.length);
                os.writeInt(buckets.bucketsPage);
                os.writeInt(buckets.capacity);
                os.writeInt(buckets.active);
            } catch (IOException e) {
                throw new IOPagingException(e);
            }
            
            Buffer buffer = os.toBuffer();
            paged.write(page, buffer);
            return Collections.emptyList();
        }
        @Override
        public Buckets<Key, Value> load(Paged paged, int page) {
            Buckets<Key, Value> buckets = new Buckets<Key, Value>(HashIndex.this);
            Buffer buffer = new Buffer(HEADER_SIZE);
            paged.read(page, buffer);
            DataByteArrayInputStream is = new DataByteArrayInputStream(buffer);
            
            Buffer magic = new Buffer(MAGIC.length);
            is.readFully(magic.data, magic.offset, magic.length);
            if (!magic.equals(MAGIC)) {
                throw new IndexException("Not a hash page");
            }
            buckets.bucketsPage = is.readInt();
            buckets.capacity = is.readInt();
            buckets.active = is.readInt();
            
            return buckets;
        }
        
        @Override
        public List<Integer> remove(Paged paged, int page) {
            return Collections.emptyList();
        }
        
    };

}
