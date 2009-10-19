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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.DataByteArrayInputStream;
import org.apache.activemq.util.buffer.DataByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawtdb.api.BTreeIndexFactory;
import org.apache.hawtdb.api.HashIndexFactory;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.IndexException;
import org.apache.hawtdb.api.Paged;


/**
 * Hash Index implementation.  The hash buckets store entries in a b+tree.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndex<Key,Value> implements Index<Key,Value> {
    
    private static final Log LOG = LogFactory.getLog(HashIndex.class);

//    static private class Header extends Struct {
//        public final UTF8String magic = new UTF8String(4);
//        public final Signed32 page = new Signed32();
//        public final Signed32 capacity = new Signed32();
//        public final Signed32 size = new Signed32();
//        public final Signed32 active = new Signed32();
//        
//        static Header create(ByteBuffer buffer) {
//            Header header = new Header();
//            header.setByteBuffer(buffer, buffer.position());
//            return header;
//        }
//    }
    

    /** 
     * This is the data stored in the index header.  It knows where
     * the hash buckets are stored at an keeps usage statistics about
     * those buckets. 
     */
    static private class Buckets<Key,Value> {
        
        public static final int HEADER_SIZE = headerSize();
        public static final Buffer MAGIC = new Buffer(new byte[] {'h', 'a', 's', 'h'});

        final HashIndex<Key,Value> index;
        int bucketsPage=-1;
        int active;
        int capacity;
        int size;
        
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
            this.size = 0;
            this.active = 0;
            this.capacity = capacity;
            this.bucketsPage = index.paged.allocator().alloc(capacity);
            for (int i = 0; i < capacity; i++) {
                index.BIN_FACTORY.create(index.paged, (bucketsPage + i));
            }
            calcThresholds();
            store();
        }
        
        public void destroy() {
            clear();
            index.paged.allocator().free(bucketsPage, capacity);
        }
        
        public void clear() {
            for (int i = 0; i < index.buckets.capacity; i++) {
                index.buckets.bucket(i).clear();
            }
            index.buckets.size = 0;
            index.buckets.active = 0;
            index.buckets.calcThresholds();
        }
        
        private static int headerSize() {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream();
            new Buckets<Object, Object>(null).writeExternal(os);
            return os.toBuffer().getLength();
        }

        void store() {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(HEADER_SIZE);
            writeExternal(os);
            Buffer buffer = os.toBuffer();
            index.paged.write(index.page, buffer);
        }

        void load() {
            Buffer buffer = new Buffer(HEADER_SIZE);
            index.paged.read(index.page, buffer);
            DataByteArrayInputStream is = new DataByteArrayInputStream(buffer);
            readExternal(is);
        }
        
        private void writeExternal(DataByteArrayOutputStream os) {
            try {
                os.write(MAGIC.data, MAGIC.offset, MAGIC.length);
                os.writeInt(this.bucketsPage);
                os.writeInt(this.capacity);
                os.writeInt(this.size);
                os.writeInt(this.active);
            } catch (IOException e) {
                throw new IOPagingException(e);
            }
        }
        
        private void readExternal(DataByteArrayInputStream is) {
            Buffer magic = new Buffer(MAGIC.length);
            is.readFully(magic.data, magic.offset, magic.length);
            if (!magic.equals(MAGIC)) {
                throw new IndexException("Not a hash page");
            }
            this.bucketsPage = is.readInt();
            this.capacity = is.readInt();
            this.size = is.readInt();
            this.active = is.readInt();
        }        

        
        Index<Key,Value> bucket(int bucket) {
            return index.BIN_FACTORY.open(index.paged, bucketsPage+bucket);
        }

        Index<Key,Value> bucket(Key key) {
            int i = index(key);
            return index.BIN_FACTORY.open(index.paged, bucketsPage+i);
        }

        int index(Key x) {
            try {
                return Math.abs(x.hashCode()%capacity);
            } catch (ArithmeticException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                throw e;
            }
        }
        
        @Override
        public String toString() {
            return "{ page:"+bucketsPage+", size: "+size+", capacity: "+capacity+", active: "+active+", increase threshold: "+increaseThreshold+", decrease threshold: "+decreaseThreshold+" }";
        }

    }
    
    private final BTreeIndexFactory<Key, Value> BIN_FACTORY = new BTreeIndexFactory<Key, Value>();
    
    private final Paged paged;
    private final int page;
    private final int maximumBucketCapacity;
    private final int minimumBucketCapacity;
    private final int loadFactor;
    private final int initialBucketCapacity;

    private Buckets<Key,Value> buckets;

    public HashIndex(Paged paged, int page, HashIndexFactory<Key,Value> factory) {
        this.paged = paged;
        this.page = page;
        this.maximumBucketCapacity = factory.getMaximumBucketCapacity();
        this.minimumBucketCapacity = factory.getMinimumBucketCapacity();
        this.loadFactor = factory.getLoadFactor();
        this.initialBucketCapacity = factory.getBucketCapacity();
        this.BIN_FACTORY.setKeyMarshaller(factory.getKeyMarshaller());
        this.BIN_FACTORY.setValueMarshaller(factory.getValueMarshaller());
    }

    public HashIndex<Key, Value> create() {
        buckets = new Buckets<Key, Value>(this);
        buckets.create(initialBucketCapacity);
        return this;
    }

    public HashIndex<Key, Value> open() {
        buckets = new Buckets<Key, Value>(this);
        buckets.load();
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

        int originalSize = bucket.size();
        Value put = bucket.put(key,value);
        int newSize = bucket.size();

        if (newSize != originalSize) {
            buckets.size++;
            if (newSize == 1) {
                buckets.active++;
            }
            buckets.store();
        }
        
        if (buckets.active >= buckets.increaseThreshold) {
            newSize = Math.min(this.maximumBucketCapacity, buckets.capacity*4);
            if(buckets.capacity!=newSize) {
                this.changeCapacity(newSize);
            }
        }
        return put;
    }
    
    public Value remove(Key key) {
        Index<Key, Value> bucket = buckets.bucket(key);
        int originalSize = bucket.size();
        Value rc = bucket.remove(key);
        int newSize = bucket.size();
        
        if (newSize != originalSize) {
            buckets.size--;
            if (newSize == 0) {
                buckets.active--;
            }
            buckets.store();
        }

        if (buckets.active <= buckets.decreaseThreshold) {
            newSize = Math.max(minimumBucketCapacity, buckets.capacity/2);
            if(buckets.capacity!=newSize) {
                changeCapacity(newSize);
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
    
    public int size() {
        return buckets.size;
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
        next.size = buckets.size;
        
        buckets.destroy();
        buckets = next;
        LOG.debug("Resizing done.  New bins start at: "+buckets.bucketsPage);        
    }

    public String toString() {
        return "{ page: "+page+", buckets: "+buckets+" }";
    }
}
