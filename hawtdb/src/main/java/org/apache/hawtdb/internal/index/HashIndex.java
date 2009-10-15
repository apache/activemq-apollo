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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.util.marshaller.Marshaller;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.Paged;


/**
 * Hash Index implementation.  The hash buckets use a BTree.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndex<Key,Value> implements Index<Key,Value> {
    
    private static final Log LOG = LogFactory.getLog(HashIndex.class);
        
    public static final String PROPERTY_PREFIX = HashIndex.class.getName()+".";
    public static final int DEFAULT_BIN_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_BIN_CAPACITY", "1024"));
    public static final int DEFAULT_MAXIMUM_BIN_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_MAXIMUM_BIN_CAPACITY", "16384"));
    public static final int DEFAULT_MINIMUM_BIN_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_MINIMUM_BIN_CAPACITY", "16"));
    public static final int DEFAULT_LOAD_FACTOR = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_LOAD_FACTOR", "75"));

    static class Factory<Key, Value> {
        private Marshaller<Key> keyMarshaller;
        private Marshaller<Value> valueMarshaller;
        private int maximumBinCapacity = DEFAULT_MAXIMUM_BIN_CAPACITY;
        private int minimumBinCapacity = DEFAULT_MINIMUM_BIN_CAPACITY;
        private int loadFactor = DEFAULT_LOAD_FACTOR;

        public HashIndex<Key, Value> open(Paged paged, int page) {
            return docreate(paged, page).open();
        }

        public HashIndex<Key, Value> create(Paged paged, int page) {
            return docreate(paged, page).create();
        }

        private HashIndex<Key, Value> docreate(Paged paged, int page) {
            assertFieldsSet();
            return new HashIndex<Key, Value>(paged, page, keyMarshaller, valueMarshaller, maximumBinCapacity, minimumBinCapacity, loadFactor);
        }

        private void assertFieldsSet() {
            if (keyMarshaller == null) {
                throw new IllegalArgumentException("The key marshaller must be set before calling open");
            }
            if (valueMarshaller == null) {
                throw new IllegalArgumentException("The key marshaller must be set before calling open");
            }
        }

        public Marshaller<Key> getKeyMarshaller() {
            return keyMarshaller;
        }

        public void setKeyMarshaller(Marshaller<Key> keyMarshaller) {
            this.keyMarshaller = keyMarshaller;
        }

        public Marshaller<Value> getValueMarshaller() {
            return valueMarshaller;
        }

        public void setValueMarshaller(Marshaller<Value> valueMarshaller) {
            this.valueMarshaller = valueMarshaller;
        }

        public int getMaximumBinCapacity() {
            return maximumBinCapacity;
        }

        public void setMaximumBinCapacity(int maximumBinCapacity) {
            this.maximumBinCapacity = maximumBinCapacity;
        }

        public int getMinimumBinCapacity() {
            return minimumBinCapacity;
        }

        public void setMinimumBinCapacity(int minimumBinCapacity) {
            this.minimumBinCapacity = minimumBinCapacity;
        }

        public int getLoadFactor() {
            return loadFactor;
        }

        public void setLoadFactor(int loadFactor) {
            this.loadFactor = loadFactor;
        }
        
    }
    
    final BTreeIndex.Factory<Key, Value> BIN_FACTORY = new BTreeIndex.Factory<Key, Value>();

    final Paged paged;
    final int page;
    final int maximumBinCapacity;
    final int minimumBinCapacity;
    private final int loadFactor;

    private HashBins bins;
    int increaseThreshold;
    int decreaseThreshold;

    public HashIndex(Paged paged, int page, Marshaller<Key> keyMarshaller, Marshaller<Value> valueMarshaller, int maximumBinCapacity, int minimumBinCapacity, int loadFactor) {
        this.paged = paged;
        this.page = page;
        this.maximumBinCapacity = maximumBinCapacity;
        this.minimumBinCapacity = minimumBinCapacity;
        this.loadFactor = loadFactor;
        this.BIN_FACTORY.setKeyMarshaller(keyMarshaller);
        this.BIN_FACTORY.setValueMarshaller(valueMarshaller);
    }

    public HashIndex<Key, Value> create() {
        this.bins = new HashBins();
        this.bins.create(this, DEFAULT_BIN_CAPACITY);
        paged.put(HashBins.ENCODER_DECODER, page, bins);
        calcThresholds();
        return this;
    }

    public HashIndex<Key, Value> open() {
        this.bins = paged.get(HashBins.ENCODER_DECODER, page);
        calcThresholds();
        return this;
    }

    public Value get(Key key) {
        return bins.bin(this, key).get(key);
    }
    
    public boolean containsKey(Key key) {
        return bins.bin(this, key).containsKey(key);
    }

    public Value put(Key key, Value value) {
        Value put = bins.put(this, key, value);
        if (bins.active >= this.increaseThreshold) {
            int newSize = Math.min(this.maximumBinCapacity, bins.capacity*2);
            if(bins.capacity!=newSize) {
                this.resize(newSize);
            }
        }
        return put;
    }
    
    public Value remove(Key key) {
        Value rc = bins.remove(this, key);
        if (bins.active <= this.decreaseThreshold) {
            int newSize = Math.max(minimumBinCapacity, bins.capacity/2);
            if(bins.capacity!=newSize) {
                resize(newSize);
            }
        }
        return rc;
    }

    public void clear() {
        bins.clear(this);
        if (bins.active <= this.decreaseThreshold) {
            int newSize = Math.max(minimumBinCapacity, bins.capacity/2);
            if(bins.capacity!=newSize) {
                resize(newSize);
            }
        }
    }
    
    public Iterator<Entry<Key, Value>> iterator() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    public int size() {
        return bins.size;
    }
    
    public void destroy() {
        bins.destroy(this);
        bins = null;
    }

    public String toString() {
        return "{ page: "+page+", bins: "+bins+" }";
    }

    // /////////////////////////////////////////////////////////////////
    // Implementation Methods
    // /////////////////////////////////////////////////////////////////
    void resize(final int capacity) {
        LOG.debug("Resizing to: "+capacity);
        
        HashBins newBins = new HashBins();
        newBins.create(this, capacity);

        // Copy the data from the old bins to the new bins.
        for (int i = 0; i < bins.capacity; i++) {
            Index<Key, Value> bin = bins.bin(this, i);
            for (Map.Entry<Key, Value> entry : bin) {
                newBins.put(this, entry.getKey(), entry.getValue());
            }
        }
        
        bins.destroy(this);
        bins = newBins;
        calcThresholds();
        LOG.debug("Resizing done.  New bins start at: "+bins.page);        
    }

    private void calcThresholds() {
        increaseThreshold = (bins.capacity * loadFactor)/100;
        decreaseThreshold = (bins.capacity * loadFactor * loadFactor ) / 20000;
    }

    public int getPage() {
        return page;
    }
}
