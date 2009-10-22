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
import org.apache.hawtdb.internal.index.BTreeIndex;

/**
 * This object is used to create variable magnitude b+tree indexes. 
 * 
 * A b+tree can be used for set or map-based indexing. Leaf
 * nodes are linked together for faster iteration of the values.
 * 
 * <br>
 * The variable magnitude attribute means that the b+tree attempts 
 * to store as many values and pointers on one page as is possible.
 * 
 * <br>
 * It will act as a simple-prefix b+tree if a prefixer is configured.
 * 
 * <br>
 * In a simple-prefix b+tree, instead of promoting actual keys to branch pages, when
 * leaves are split, a shortest-possible separator is generated at the pivot.
 * That separator is what is promoted to the parent branch (and continuing up
 * the list). As a result, actual keys and pointers can only be found at the
 * leaf level. This also affords the index the ability to ignore costly merging
 * and redistribution of pages when deletions occur. Deletions only affect leaf
 * pages in this implementation, and so it is entirely possible for a leaf page
 * to be completely empty after all of its keys have been removed.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeIndexFactory<Key, Value> implements IndexFactory<Key, Value> {

    private Marshaller<Key> keyMarshaller;
    private Marshaller<Value> valueMarshaller;
    private boolean deferredEncoding=true;
    private Prefixer<Key> prefixer;

    public Index<Key, Value> create(Paged paged, int page) {
        BTreeIndex<Key, Value> index = createInstance(paged, page);
        index.create();
        return index;
    }
    
    @Override
    public String toString() {
        return "{ deferredEncoding: "+deferredEncoding+" }";
    }
    
    public Index<Key, Value> open(Paged paged, int page) {
        return createInstance(paged, page);
    }

    private BTreeIndex<Key, Value> createInstance(Paged paged, int page) {
        if (keyMarshaller == null) {
            throw new IllegalArgumentException("The key marshaller must be set before calling open");
        }
        if (valueMarshaller == null) {
            throw new IllegalArgumentException("The key marshaller must be set before calling open");
        }
        return new BTreeIndex<Key, Value>(paged, page, this);
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

    public boolean isDeferredEncoding() {
        return deferredEncoding;
    }

    public void setDeferredEncoding(boolean deferredEncoding) {
        this.deferredEncoding = deferredEncoding;
    }

    public Prefixer<Key> getPrefixer() {
        return prefixer;
    }

    public void setPrefixer(Prefixer<Key> prefixer) {
        this.prefixer = prefixer;
    }
    
}