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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.IndexException;
import org.apache.hawtdb.api.IndexVisitor;
import org.apache.hawtdb.api.Paged;
import org.apache.hawtdb.api.Prefixer;
import org.apache.hawtdb.internal.page.Extent;
import org.apache.hawtdb.internal.page.ExtentInputStream;
import org.apache.hawtdb.internal.page.ExtentOutputStream;
import org.apache.hawtdb.internal.util.Ranges;


/**
 * The BTreeNode class represents a node in the BTree object graph. It is stored
 * in one Page of a PageFile.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class BTreeNode<Key, Value> {

    private static final Object [] EMPTY_ARRAY = new Object[]{};
    
    @SuppressWarnings("unchecked")
    private static final Data EMPTY_DATA = new Data();
    
    public static final Buffer BRANCH_MAGIC = new Buffer(new byte[]{ 'b', 'b'});
    public static final Buffer LEAF_MAGIC = new Buffer(new byte[]{ 'b', 'l'});
    
    /**
     * This is the persistent data of each node.  Declared immutable so that 
     * it can behave nicely in the page cache.  
     * 
     * TODO: Consider refactoring into branch/leaf sub classes.
     * 
     * @param <Key>
     * @param <Value>
     */
    static class Data<Key, Value> {

        // Order list of keys in the node
        final Key[] keys;

        // Values associated with the Keys. Null if this is a branch node.
        final Value[] values;

        // nodeId pointers to children BTreeNodes. Null if this is a leaf node.
        final int[] children;

        // The next leaf node after this one. Used for fast iteration of the
        // entries. -1 if this is the last node.
        final int next;
        
        @SuppressWarnings("unchecked")
        public Data() {
            this((Key[])EMPTY_ARRAY, null, (Value[])EMPTY_ARRAY, -1);
        }
        
        public Data(Key[] keys, int[] children, Value[] values, int next) {
            this.keys = keys;
            this.values = values;
            this.children = children;
            this.next = next;
        }        
        
        @Override
        public String toString() {
            return "{ next: "+next+", type: "+(isBranch()?"branch":"leaf")+", keys: "+Arrays.toString(keys)+" }";
        }
        
        public boolean isBranch() {
            return children != null;
        }

        public Data<Key, Value> values(Value[] values) {
            return new Data<Key, Value>(keys, children, values, next);
        }

        public Data<Key, Value> children(int[] children) {
            return new Data<Key, Value>(keys, children, values, next);
        }

        public Data<Key, Value> next(int next) {
            return new Data<Key, Value>(keys, children, values, next);
        }
        
        public Data<Key, Value> change(Key[] keys, int[] children, Value[] values) {
            return new Data<Key, Value>(keys, children, values, next);
        }
        
        public Data<Key, Value> branch(Key[] keys, int[] children) {
            return new Data<Key, Value>(keys, children, null, next);
        }
        
        public Data<Key, Value> leaf(Key[] keys, Value[] values) {
            return new Data<Key, Value>(keys, null, values, next);
        }
        
        public Data<Key, Value> leaf(Key[] keys, Value[] values, int next) {
            return new Data<Key, Value>(keys, null, values, next);
        }

    }
    
    static <Key, Value> int estimatedSize(BTreeIndex<Key, Value> index, Data<Key, Value> data) {
        int rc = 6; // magic + key count..
        
        // calculate the size of the keys.
        int v = index.getKeyMarshaller().getFixedSize();
        if( v >=0 ) {
            rc += v*data.keys.length;
        } else {
            for (Key key : data.keys) {
                rc += index.getKeyMarshaller().estimatedSize(key);
            }
        }
        
        if( data.isBranch() ) {
            // calculate the size of the children.
            rc += 4*data.children.length;
        } else {
            // calculate the size of the values.
            v = index.getValueMarshaller().getFixedSize();
            if( v >=0 ) {
                rc += v*data.values.length;
            } else {
                for (Value value : data.values) {
                    rc += index.getValueMarshaller().estimatedSize(value);
                }
            }
            rc += 4; // for the next pointer.
        }
        
        return rc;
    }
    
    static <Key, Value> void write(DataOutput os, BTreeIndex<Key, Value> index, Data<Key, Value> data) throws IOException {
        if( data.isBranch() ) {
            os.write(BRANCH_MAGIC.data, BRANCH_MAGIC.offset, BRANCH_MAGIC.length);
        } else {
            os.write(LEAF_MAGIC.data, LEAF_MAGIC.offset, LEAF_MAGIC.length);
        }
        
        int count = data.keys.length;
        os.writeShort(count);
        for (int i = 0; i < data.keys.length; i++) {
            index.getKeyMarshaller().writePayload(data.keys[i], os);
        }

        if (data.isBranch()) {
            for (int i = 0; i < count + 1; i++) {
                os.writeInt(data.children[i]);
            }
        } else {
            for (int i = 0; i < count; i++) {
                index.getValueMarshaller().writePayload(data.values[i], os);
            }
            os.writeInt(data.next);
        }
    }
    
    @SuppressWarnings("unchecked") 
    static <Key, Value> Data<Key, Value> read(DataInput is, BTreeIndex<Key, Value> index) throws IOException {
        Buffer magic = new Buffer(BRANCH_MAGIC.length);
        is.readFully(magic.data, magic.offset, magic.length);
        boolean branch;
        if( magic.equals(BRANCH_MAGIC)) {
            branch = true;
        } else if( magic.equals(LEAF_MAGIC)) {
            branch = false;
        } else {
            throw new IndexException("Page did not contain the expected btree headers");
        }
        
        int count = is.readShort();
        Key[] keys = (Key[]) new Object[count];
        int[] children = null;
        Value[] values = null;
        int next = -1;

        for (int i = 0; i < count; i++) {
            keys[i] = index.getKeyMarshaller().readPayload(is);
        }

        if (branch) {
            children = new int[count + 1];
            for (int i = 0; i < count + 1; i++) {
                children[i] = is.readInt();
            }
        } else {
            values = (Value[]) new Object[count];
            for (int i = 0; i < count; i++) {
                values[i] = index.getValueMarshaller().readPayload(is);
            }
            next = is.readInt();
        }
        return new Data<Key, Value>(keys, children, values, next);
    }
    
    static public class DataEncoderDecoder<Key, Value> implements EncoderDecoder<Data<Key, Value>> {
        private final BTreeIndex<Key, Value> index;

        public DataEncoderDecoder(BTreeIndex<Key, Value> index) {
            this.index = index;
        }

        public List<Integer> store(Paged paged, int page, Data<Key, Value> data) {
            short count = (short) data.keys.length; // cast may truncate value...
            if (count != data.keys.length) {
                throw new IndexException("Too many keys");
            }

            // The node will be stored in an extent. This allows us to easily
            // support huge nodes.
            // The first extent is only 1 page long, extents linked off
            // the first page will be up to 128 pages long.
            ExtentOutputStream eos = new ExtentOutputStream(paged, page, (short) 1, (short) 128);
            DataOutputStream os = new DataOutputStream(eos);
            try {
                write(os, index, data);
                os.close();
            } catch (IOException e) {
                throw new IndexException(e);
            }

            Ranges pages = eos.getPages();
            pages.remove(page);
            if (pages.isEmpty()) {
                return Collections.emptyList();
            }

            return pages.values();
        }

        public Data<Key, Value> load(Paged paged, int page) {
            ExtentInputStream eis = new ExtentInputStream(paged, page);
            DataInputStream is = new DataInputStream(eis);
            try {
                return read(is, index);
            } catch (IOException e) {
                throw new IndexException(e);
            } finally {
                try {
                    is.close();
                } catch (Throwable ignore) {
                }
            }

        }

        public List<Integer> remove(Paged paged, int page) {
            return Extent.freeLinked(paged, page);
        }

    }

    BTreeNode<Key, Value> parent;
    // The persistent data of the node.
    Data<Key, Value> data;
    // The page associated with this node
    int page;
    boolean storedInExtent;
    
    @SuppressWarnings("unchecked")
    public BTreeNode(BTreeNode<Key, Value> parent, int page) {
        this(parent, page, EMPTY_DATA);
    }

    public BTreeNode(BTreeNode<Key, Value> parent, int page, Data<Key, Value> data) {
        this.parent = parent;
        this.page = page;
        this.data = data;
    }
    
    
    
    /**
     * Internal (to the BTreeNode) method. Because this method is called only by
     * BTreeNode itself, no synchronization done inside of this method.
     * 
     * @throws IOException
     */
    private BTreeNode<Key, Value> getChild(BTreeIndex<Key, Value> index, int idx) {
        if (data.isBranch() && idx >= 0 && idx < data.children.length) {
            BTreeNode<Key, Value> result = index.loadNode(this, data.children[idx]);
            return result;
        } else {
            return null;
        }
    }

    public Value remove(BTreeIndex<Key, Value> index, Key key) {

        if (data.isBranch()) {
            int idx = Arrays.binarySearch(data.keys, key);
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            BTreeNode<Key, Value> child = getChild(index, idx);
            if (child.getPage() == index.getPage()) {
                throw new IndexException("BTree corrupted: Cylce detected.");
            }
            Value rc = child.remove(index, key);

            // child node is now empty.. remove it from the branch node.
            if (child.data.keys.length == 0) {

                // If the child node is a branch, promote
                if (child.data.isBranch()) {
                    // This is cause branches are never really empty.. they just
                    // go down to 1 child..
                    data = data.children(arrayUpdate(data.children, idx, child.data.children[0]));
                } else {

                    // The child was a leaf. Then we need to actually remove it
                    // from this branch node..

                    // We need to update the previous child's next pointer to
                    // skip over the child being removed....
                    int prevIdx = idx - 1;
                    if (idx > 0 && data.children.length > 1) {
                        getChild(index, prevIdx).setNext(index, child.data.next);
                    }

                    if (idx < data.children.length - 1) {
                        // Delete it and key to the right.
                        data = data.branch(arrayDelete(data.keys, idx), arrayDelete(data.children, idx));
                    } else {
                        // It was the last child.. Then delete it and key to the
                        // left
                        data = data.branch(arrayDelete(data.keys, prevIdx), arrayDelete(data.children, idx));
                    }

                    // If we are the root node, and only have 1 child left. Then
                    // make the root be the leaf node.
                    if (data.children.length == 1 && parent == null) {
                        child = getChild(index, 0);
                        data = data.change(child.data.keys, child.data.children, child.data.values);
                        // free up the page..
                        index.free(child);
                    }

                }
                index.storeNode(this);
            }

            return rc;
        } else {
            int idx = Arrays.binarySearch(data.keys, key);
            if (idx < 0) {
                return null;
            } else {
                Value oldValue = data.values[idx];
                data = data.leaf(arrayDelete(data.keys, idx), arrayDelete(data.values, idx));

                if (data.keys.length == 0 && parent != null) {
                    index.free(this);
                } else {
                    index.storeNode(this);
                }

                return oldValue;
            }
        }
    }

    private void setNext(BTreeIndex<Key, Value> index, int next) {
        data = data.next(next);
        index.storeNode(this);
    }

    public Value put(BTreeIndex<Key, Value> index, Key key, Value value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (data.isBranch()) {
            return getLeafNode(index, this, key).put(index, key, value);
        } else {
            int idx = Arrays.binarySearch(data.keys, key);

            Value oldValue = null;
            if (idx >= 0) {
                // Key was found... Overwrite
                oldValue = data.values[idx];
                data = data.leaf(data.keys, arrayUpdate(data.values, idx, value));
            } else {
                // Key was not found, Insert it
                idx = -(idx + 1);
                data = data.leaf(arrayInsert(data.keys, key, idx), arrayInsert(data.values, value, idx));
            }

//            if (splitNeeded()) {
//                split(index);
//            } else {
                if( !index.storeNode(this) ) {
                    split(index);
                }
//            }

            return oldValue;
        }
    }

    private void promoteValue(BTreeIndex<Key, Value> index, Key key, int nodeId) {

        int idx = Arrays.binarySearch(data.keys, key);
        idx = idx < 0 ? -(idx + 1) : idx + 1;
        data = data.branch(arrayInsert(data.keys, key, idx), arrayInsert(data.children, nodeId, idx + 1));

//        if (splitNeeded()) {
//            split(index);
//        } else {
            if ( !index.storeNode(this) ) {
                // overflow.. 
                split(index);
            }
//        }

    }

    /**
     * Internal to the BTreeNode method
     */
    private void split(BTreeIndex<Key, Value> index) {
        Key[] leftKeys;
        Key[] rightKeys;
        Value[] leftValues = null;
        Value[] rightValues = null;
        int[] leftChildren = null;
        int[] rightChildren = null;
        Key separator;

        int vc = data.keys.length;
        int pivot = vc / 2;

        // Split the node into two nodes
        if (data.isBranch()) {

            leftKeys = createKeyArray(pivot);
            leftChildren = new int[leftKeys.length + 1];
            rightKeys = createKeyArray(vc - (pivot + 1));
            rightChildren = new int[rightKeys.length + 1];

            System.arraycopy(data.keys, 0, leftKeys, 0, leftKeys.length);
            System.arraycopy(data.children, 0, leftChildren, 0, leftChildren.length);
            System.arraycopy(data.keys, leftKeys.length + 1, rightKeys, 0, rightKeys.length);
            System.arraycopy(data.children, leftChildren.length, rightChildren, 0, rightChildren.length);

            // Is it a Simple Prefix BTree??
            Prefixer<Key> prefixer = index.getPrefixer();
            if (prefixer != null) {
                separator = prefixer.getSimplePrefix(leftKeys[leftKeys.length - 1], rightKeys[0]);
            } else {
                separator = data.keys[leftKeys.length];
            }

        } else {

            leftKeys = createKeyArray(pivot);
            leftValues = createValueArray(leftKeys.length);
            rightKeys = createKeyArray(vc - pivot);
            rightValues = createValueArray(rightKeys.length);

            System.arraycopy(data.keys, 0, leftKeys, 0, leftKeys.length);
            System.arraycopy(data.values, 0, leftValues, 0, leftValues.length);
            System.arraycopy(data.keys, leftKeys.length, rightKeys, 0, rightKeys.length);
            System.arraycopy(data.values, leftValues.length, rightValues, 0, rightValues.length);

            // separator = getSeparator(leftVals[leftVals.length - 1],
            // rightVals[0]);
            separator = rightKeys[0];

        }

        // Promote the pivot to the parent branch
        if (parent == null) {

            // This can only happen if this is the root
            BTreeNode<Key, Value> lNode = index.createNode(this);
            BTreeNode<Key, Value> rNode = index.createNode(this);

            if (data.isBranch()) {
                rNode.data = data.branch(rightKeys, rightChildren);
                lNode.data = data.branch(leftKeys, leftChildren);
            } else {
                rNode.data = data.leaf(rightKeys, rightValues);
                lNode.data = data.leaf(leftKeys, leftValues, rNode.getPage());
            }

            Key[] v = createKeyArray(1);
            v[0] = separator;
            data = data.branch(v, new int[] { lNode.getPage(), rNode.getPage() });

            index.storeNode(this);
            index.storeNode(rNode);
            index.storeNode(lNode);

        } else {
            BTreeNode<Key, Value> rNode;

            if (data.isBranch()) {
                rNode = index.createNode(parent, data.branch(rightKeys, rightChildren));
                data = data.branch(leftKeys, leftChildren);
            } else {
                rNode = index.createNode(parent, data.leaf(rightKeys, rightValues, data.next));
                data = data.leaf(leftKeys, leftValues, rNode.getPage());
            }

            index.storeNode(this);
            index.storeNode(rNode);
            parent.promoteValue(index, separator, rNode.getPage());
        }
    }

    public void printStructure(BTreeIndex<Key, Value> index, PrintWriter out, String prefix) {
        if (prefix.length() > 0 && parent == null) {
            throw new IllegalStateException("Cycle back to root node detected.");
        }

        if (data.isBranch()) {
            for (int i = 0; i < data.children.length; i++) {
                BTreeNode<Key, Value> child = getChild(index, i);
                if (i == data.children.length - 1) {
                    out.println(prefix + "\\- " + child.getPage() + (child.data.isBranch() ? " (" + child.data.children.length + ")" : ""));
                    child.printStructure(index, out, prefix + "   ");
                } else {
                    out.println(prefix + "|- " + child.getPage() + (child.data.isBranch() ? " (" + child.data.children.length + ")" : "") + " : " + data.keys[i]);
                    child.printStructure(index, out, prefix + "   ");
                }
            }
        }
    }

    public int getMinLeafDepth(BTreeIndex<Key, Value> index, int depth) {
        depth++;
        if (data.isBranch()) {
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < data.children.length; i++) {
                min = Math.min(min, getChild(index, i).getMinLeafDepth(index, depth));
            }
            return min;
        } else {
            // print(depth*2, "- "+page.getPageId());
            return depth;
        }
    }
    
    public int size(BTreeIndex<Key, Value> index) {
        int rc=0;
        
        BTreeNode<Key, Value> node = this;
        while (node.data.isBranch()) {
            node = node.getChild(index, 0);
        }
        while (node!=null) {
            rc += node.data.values.length;
            if( node.data.next!= -1 ) {
                node = index.loadNode(null, node.data.next);
            } else {
                node = null;
            }
        }
        return rc;
    }
    
    public boolean isEmpty(BTreeIndex<Key, Value> index) {
        return data.keys.length==0;
    }    

    public int getMaxLeafDepth(BTreeIndex<Key, Value> index, int depth) {
        depth++;
        if (data.isBranch()) {
            int v = 0;
            for (int i = 0; i < data.children.length; i++) {
                v = Math.max(v, getChild(index, i).getMaxLeafDepth(index, depth));
            }
            depth = v;
        }
        return depth;
    }

    public Value get(BTreeIndex<Key, Value> index, Key key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (data.isBranch()) {
            return getLeafNode(index, this, key).get(index, key);
        } else {
            int idx = Arrays.binarySearch(data.keys, key);
            if (idx < 0) {
                return null;
            } else {
                return data.values[idx];
            }
        }
    }

    public void visit(BTreeIndex<Key, Value> index, IndexVisitor<Key, Value> visitor) {
        if (visitor == null) {
            throw new IllegalArgumentException("Visitor cannot be null");
        }

        if (visitor.isSatiated()) {
            return;
        }

        if (data.isBranch()) {
            for (int i = 0; i < this.data.children.length; i++) {
                Key key1 = null;
                if (i != 0) {
                    key1 = data.keys[i - 1];
                }
                Key key2 = null;
                if (i != this.data.children.length - 1) {
                    key2 = data.keys[i];
                }
                if (visitor.isInterestedInKeysBetween(key1, key2)) {
                    BTreeNode<Key, Value> child = getChild(index, i);
                    child.visit(index, visitor);
                }
            }
        } else {
            visitor.visit(Arrays.asList(data.keys), Arrays.asList(data.values));
        }
    }

    public Map.Entry<Key, Value> getFirst(BTreeIndex<Key, Value> index) {
        BTreeNode<Key, Value> node = this;
        while (node.data.isBranch()) {
            node = node.getChild(index, 0);
        }
        if (node.data.values.length > 0) {
            return new MapEntry<Key, Value>(node.data.keys[0], node.data.values[0]);
        } else {
            return null;
        }
    }

    public Map.Entry<Key, Value> getLast(BTreeIndex<Key, Value> index) {
        BTreeNode<Key, Value> node = this;
        while (node.data.isBranch()) {
            node = node.getChild(index, node.data.children.length - 1);
        }
        if (node.data.values.length > 0) {
            int idx = node.data.values.length - 1;
            return new MapEntry<Key, Value>(node.data.keys[idx], node.data.values[idx]);
        } else {
            return null;
        }
    }

    public BTreeNode<Key, Value> getFirstLeafNode(BTreeIndex<Key, Value> index) {
        BTreeNode<Key, Value> node = this;
        while (node.data.isBranch()) {
            node = node.getChild(index, 0);
        }
        return node;
    }

    public Iterator<Map.Entry<Key, Value>> iterator(BTreeIndex<Key, Value> index, final Key startKey) {
        if (startKey == null) {
            return iterator(index);
        }
        if (data.isBranch()) {
            return getLeafNode(index, this, startKey).iterator(index, startKey);
        } else {
            int idx = Arrays.binarySearch(data.keys, startKey);
            if (idx < 0) {
                idx = -(idx + 1);
            }
            return new BTreeIterator<Key, Value>(index, this, idx);
        }
    }

    public Iterator<Map.Entry<Key, Value>> iterator(final BTreeIndex<Key, Value> index) {
        return new BTreeIterator<Key, Value>(index, getFirstLeafNode(index), 0);
    }

    @SuppressWarnings("unchecked")
    public void clear(BTreeIndex<Key, Value> index) {
        if (data.isBranch()) {
            for (int i = 0; i < data.children.length; i++) {
                BTreeNode<Key, Value> node = index.loadNode(this, data.children[i]);
                node.clear(index);
                index.free(node);
            }
        }
        // Reset the root node to be a leaf.
        if (parent == null) {
            data = data.leaf((Key[])EMPTY_ARRAY, (Value[])EMPTY_ARRAY, -1);
            index.storeNode(this);
        }
    }

    private static <Key, Value> BTreeNode<Key, Value> getLeafNode(BTreeIndex<Key, Value> index, final BTreeNode<Key, Value> node, Key key) {
        BTreeNode<Key, Value> current = node;
        while (true) {
            if (current.data.isBranch()) {
                int idx = Arrays.binarySearch(current.data.keys, key);
                idx = idx < 0 ? -(idx + 1) : idx + 1;
                BTreeNode<Key, Value> child = current.getChild(index, idx);

                // A little cycle detection for sanity's sake
                if (child == node) {
                    throw new IndexException("BTree corrupted: Cylce detected.");
                }

                current = child;
            } else {
                break;
            }
        }
        return current;
    }

    public boolean contains(BTreeIndex<Key, Value> index, Key key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (data.isBranch()) {
            return getLeafNode(index, this, key).contains(index, key);
        } else {
            int idx = Arrays.binarySearch(data.keys, key);
            if (idx < 0) {
                return false;
            } else {
                return true;
            }
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Implementation methods
    // /////////////////////////////////////////////////////////////////

    boolean allowPageOverflow() {
        return data.keys.length < 4;
    }
    
//    private boolean splitNeeded() {
//        if (pageCount > 1 && data.keys.length > 1) {
//            if (pageCount > 128 || !allowPageOverflow() ) {
//                return true;
//            }
//        }
//        return false;
//    }

    @SuppressWarnings("unchecked")
    private Key[] createKeyArray(int size) {
        return (Key[]) new Object[size];
    }

    @SuppressWarnings("unchecked")
    private Value[] createValueArray(int size) {
        return (Value[]) new Object[size];
    }

    @SuppressWarnings("unchecked")
    static private <T> T[] arrayUpdate(T[] vals, int idx, T value) {
        T[] newVals = (T[]) new Object[vals.length];
        System.arraycopy(vals, 0, newVals, 0, vals.length);
        newVals[idx] = value;
        return newVals;
    }
    
    static private int[] arrayUpdate(int[] vals, int idx, int value) {
        int[] newVals = new int[vals.length];
        System.arraycopy(vals, 0, newVals, 0, vals.length);
        newVals[idx] = value;
        return newVals;
    }    

    @SuppressWarnings("unchecked")
    static private <T> T[] arrayDelete(T[] vals, int idx) {
        T[] newVals = (T[]) new Object[vals.length - 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        if (idx < newVals.length) {
            System.arraycopy(vals, idx + 1, newVals, idx, newVals.length - idx);
        }
        return newVals;
    }

    static private int[] arrayDelete(int[] vals, int idx) {
        int[] newVals = new int[vals.length - 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        if (idx < newVals.length) {
            System.arraycopy(vals, idx + 1, newVals, idx, newVals.length - idx);
        }
        return newVals;
    }

    @SuppressWarnings("unchecked")
    static private <T> T[] arrayInsert(T[] vals, T val, int idx) {
        T[] newVals = (T[]) new Object[vals.length + 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        newVals[idx] = val;
        if (idx < vals.length) {
            System.arraycopy(vals, idx, newVals, idx + 1, vals.length - idx);
        }
        return newVals;
    }
    
    static private int[] arrayInsert(int[] vals, int val, int idx) {

        int[] newVals = new int[vals.length + 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        newVals[idx] = val;
        if (idx < vals.length) {
            System.arraycopy(vals, idx, newVals, idx + 1, vals.length - idx);
        }
        return newVals;
    }

    public BTreeNode<Key, Value> getParent() {
        return parent;
    }

    public int getPage() {
        return page;
    }
    public void setPage(int page) {
        this.page = page;
    }

    public int getNext() {
        return data.next;
    }

    @Override
    public String toString() {
        return "{ page: "+page+", data: "+data.toString()+" }";
    }

    public boolean isLeaf() {
        return !data.isBranch();
    }
    
    public boolean isBranch() {
        return data.isBranch();
    }


}
