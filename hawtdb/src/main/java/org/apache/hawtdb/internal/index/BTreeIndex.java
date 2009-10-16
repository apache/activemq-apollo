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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.apache.activemq.util.marshaller.Marshaller;
import org.apache.hawtdb.api.IndexVisitor;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.Paged;
import org.apache.hawtdb.internal.index.BTreeNode.Data;


/**
 * BTreeIndex represents a Variable Magnitude B+Tree in a Page File. A BTree is
 * a bit flexible in that it can be used for set or map-based indexing. Leaf
 * nodes are linked together for faster iteration of the values.
 * 
 * <br>
 * The Variable Magnitude attribute means that the BTree attempts to store as
 * many values and pointers on one page as is possible.
 * 
 * <br>
 * The implementation can optionally a be Simple-Prefix B+Tree.
 * 
 * <br>
 * For those who don't know how a Simple-Prefix B+Tree works, the primary
 * distinction is that instead of promoting actual keys to branch pages, when
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
public class BTreeIndex<Key, Value> implements Index<Key, Value> {

    static class Factory<Key, Value> {

        private Marshaller<Key> keyMarshaller;
        private Marshaller<Value> valueMarshaller;
        private boolean deferredEncoding;
        private Prefixer<Key> prefixer;

        public BTreeIndex<Key, Value> create(Paged paged, int page) {
            BTreeIndex<Key, Value> index = createInstance(paged, page);
            BTreeNode<Key, Value> root = new BTreeNode<Key, Value>(page);
            index.storeNode(root); // Store the root page..
            return index;
        }
        
        public BTreeIndex<Key, Value> open(Paged paged, int page) {
            BTreeIndex<Key, Value> index = createInstance(paged, page);
            return index;
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

    private final BTreeNode.BTreeNodeEncoderDecoder<Key, Value> PAGE_ENCODER_DECODER = new BTreeNode.BTreeNodeEncoderDecoder<Key, Value>(this);

    private final Paged paged;
    private final int page;
    private final Marshaller<Key> keyMarshaller;
    private final Marshaller<Value> valueMarshaller;
    private final Prefixer<Key> prefixer;
    private final boolean deferredEncoding;
    
    public BTreeIndex(Paged paged, int page, Factory<Key, Value> factory) {
        this.paged = paged;
        this.page = page;
        this.keyMarshaller = factory.getKeyMarshaller();
        this.valueMarshaller = factory.getValueMarshaller();
        this.deferredEncoding = factory.isDeferredEncoding();
        this.prefixer = factory.getPrefixer();
    }

    public boolean containsKey(Key key) {
        return root().contains(this, key);
    }

    public Value get(Key key) {
        return root().get(this, key);
    }

    public Value put(Key key, Value value) {
        return root().put(this, key, value);
    }

    public Value remove(Key key) {
        return root().remove(this, key);
    }

    public boolean isTransient() {
        return false;
    }

    public void clear() {
        root().clear(this);
    }

    public int getMinLeafDepth() {
        return root().getMinLeafDepth(this, 0);
    }

    public int getMaxLeafDepth() {
        return root().getMaxLeafDepth(this, 0);
    }

    public void printStructure(PrintWriter out) {
        root().printStructure(this, out, "");
    }

    public void printStructure(OutputStream out) {
        PrintWriter pw = new PrintWriter(out, false);
        root().printStructure(this, pw, "");
        pw.flush();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return root().iterator(this);
    }

    public Iterator<Map.Entry<Key, Value>> iterator(final Key initialKey) {
        return root().iterator(this, initialKey);
    }

    public void visit(IndexVisitor<Key, Value> visitor) {
        root().visit(this, visitor);
    }

    public Map.Entry<Key, Value> getFirst() {
        return root().getFirst(this);
    }

    public Map.Entry<Key, Value> getLast() {
        return root().getLast(this);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal implementation methods
    // /////////////////////////////////////////////////////////////////
    private BTreeNode<Key, Value> root() {
        return loadNode(page);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal methods made accessible to BTreeNode
    // /////////////////////////////////////////////////////////////////
    BTreeNode<Key, Value> createNode(Data<Key, Value> data) {
        return new BTreeNode<Key, Value>(paged.allocator().alloc(1), data);
    }
    
    void storeNode(BTreeNode<Key, Value> node) {
        if( deferredEncoding ) {
            paged.put(PAGE_ENCODER_DECODER, node.getPage(), node);
        } else {
            PAGE_ENCODER_DECODER.store(paged, node.getPage(), node);
        }
    }
    
    BTreeNode<Key, Value> loadNode(int page) {
        BTreeNode<Key, Value> node;
        if( deferredEncoding ) {
            node = paged.get(PAGE_ENCODER_DECODER, page);
        } else {
            node = PAGE_ENCODER_DECODER.load(paged, page);
        }
        node.setPage(page);
        return node;
    }
    
    void free( int page ) {
        if( deferredEncoding ) {
            paged.remove(PAGE_ENCODER_DECODER, page);
        } else {
            PAGE_ENCODER_DECODER.remove(paged, page);
        }
        paged.allocator().free(page, 1);
    }

    // /////////////////////////////////////////////////////////////////
    // Property Accessors
    // /////////////////////////////////////////////////////////////////

    public Paged getPaged() {
        return paged;
    }

    public int getPage() {
        return page;
    }

    public Marshaller<Key> getKeyMarshaller() {
        return keyMarshaller;
    }

    public Marshaller<Value> getValueMarshaller() {
        return valueMarshaller;
    }

    public Prefixer<Key> getPrefixer() {
        return prefixer;
    }

    public void destroy() {
        // TODO Auto-generated method stub
    }

    public int size() {
        return root().size(this);
    }

}
