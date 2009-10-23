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
package org.apache.hawtdb.api;

import org.apache.activemq.util.marshaller.Marshaller;
import org.apache.hawtdb.internal.index.HashIndex;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndexFactory<Key, Value> implements IndexFactory<Key, Value> {
    
    public static final String PROPERTY_PREFIX = HashIndex.class.getName()+".";
    public static final int DEFAULT_BUCKET_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_BUCKET_CAPACITY", "1024"));
    public static final int DEFAULT_MAXIMUM_BUCKET_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_MAXIMUM_BUCKET_CAPACITY", "16384"));
    public static final int DEFAULT_MINIMUM_BUCKET_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_MINIMUM_BUCKET_CAPACITY", "16"));
    public static final int DEFAULT_LOAD_FACTOR = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_LOAD_FACTOR", "75"));
    
    private Marshaller<Key> keyMarshaller;
    private Marshaller<Value> valueMarshaller;
    private int initialBucketCapacity = DEFAULT_BUCKET_CAPACITY;
    private int maximumBucketCapacity = DEFAULT_MAXIMUM_BUCKET_CAPACITY;
    private int minimumBucketCapacity = DEFAULT_MINIMUM_BUCKET_CAPACITY;
    private int loadFactor = DEFAULT_LOAD_FACTOR;
    private boolean deferredEncoding=true;

    public Index<Key, Value> open(Paged paged, int page) {
        return docreate(paged, page).open();
    }

    public Index<Key, Value> create(Paged paged, int page) {
        return docreate(paged, page).create();
    }

    private HashIndex<Key, Value> docreate(Paged paged, int page) {
        assertFieldsSet();
        return new HashIndex<Key, Value>(paged, page, this);
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

    public int getMaximumBucketCapacity() {
        return maximumBucketCapacity;
    }

    public void setMaximumBucketCapacity(int maximumBucketCapacity) {
        this.maximumBucketCapacity = maximumBucketCapacity;
    }

    public int getMinimumBucketCapacity() {
        return minimumBucketCapacity;
    }

    public void setMinimumBucketCapacity(int minimumBucketCapacity) {
        this.minimumBucketCapacity = minimumBucketCapacity;
    }

    public int getLoadFactor() {
        return loadFactor;
    }

    public void setLoadFactor(int loadFactor) {
        this.loadFactor = loadFactor;
    }

    public int getBucketCapacity() {
        return initialBucketCapacity;
    }

    public void setBucketCapacity(int binCapacity) {
        this.initialBucketCapacity = binCapacity;
    }

    public void setFixedCapacity(int value) {
        this.minimumBucketCapacity = this.maximumBucketCapacity = this.initialBucketCapacity = value;
    }

    public boolean isDeferredEncoding() {
        return deferredEncoding;
    }

    public void setDeferredEncoding(boolean deferredEncoding) {
        this.deferredEncoding = deferredEncoding;
    }
    
    
    
}