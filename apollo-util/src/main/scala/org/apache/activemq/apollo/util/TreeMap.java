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
package org.apache.activemq.apollo.util;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * A TreeMap that is lighter weight than the Sun implementation with
 * implementations for upper/lower/floor/ceiling accessors.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TreeMap<K, V> implements Serializable {

    private static final long serialVersionUID = 6107175734705142096L;
    
    private static final boolean RED = false;
    private static final boolean BLACK = true;

    private int count;
    private TreeEntry<K, V> root;
    private final Comparator<? super K> comparator;

    public TreeMap() {
        this.comparator = null;
    }

    @SuppressWarnings("unchecked")
    private int compare(K k1, K k2) {
        if (comparator != null) {
            return comparator.compare(k1, k2);
        } else {
            return ((Comparable<K>) k1).compareTo(k2);
        }
    }

    public TreeMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
    }

    public Comparator<? super K> comparator() {
        return comparator;
    }

    /**
     * 
     * @return The first key in the map.
     */
    public K firstKey() {
        TreeEntry<K, V> first = firstEntry();
        if (first != null) {
            return first.key;
        }
        return null;
    }

    /**
     * @return The last key in the map.
     */
    public K lastKey() {
        TreeEntry<K, V> last = lastEntry();
        if (last != null) {
            return last.key;
        }
        return null;
    }

    /**
     * Clears all elements in this map.
     */
    public void clear() {
        //TODO possible assist gc here by scanning and removing refs?
        //Will almost certain want to do this if we switch this over
        //to allowing additions to be the entries themselves.
        root = null;
        count = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(K key) {
        return getEntry(key, root) != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        for (Map.Entry<K, V> entry : entrySet()) {
            if (entry.getValue() == value) {
                return true;
            } else if (value != null && value.equals(entry)) {
                return true;
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#entrySet()
     */
    public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<Map.Entry<K, V>>() {

            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new EntryIterator();
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean contains(Object o) {
                try {
                    Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
                    TreeEntry<K, V> ours = getEntry(entry.getKey(), root);
                    if (ours != null) {
                        return ours.value == null ? entry.getValue() == null : ours.value.equals(entry.getValue());
                    } else {
                        return false;
                    }
                } catch (ClassCastException cce) {
                    return false;
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean remove(Object o) {
                return TreeMap.this.removeEntry((TreeEntry<K, V>) o) != null;
            }

            @Override
            public int size() {
                return count;
            }

            @Override
            public void clear() {
                TreeMap.this.clear();
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#get(java.lang.Object)
     */
    public V get(K key) {
        TreeEntry<K, V> node = getEntry(key, root);
        if (node != null) {
            return node.value;
        }
        return null;
    }

    public final TreeEntry<K, V> getEntry(K key) {
        return getEntry(key, root);
    }
    
    private final TreeEntry<K, V> getEntry(K key, TreeEntry<K, V> r) {
        while (r != null) {
            int c = compare(key, r.key);
            if (c == 0) {
                return r;
            } else if (c > 0) {
                r = r.right;
            } else {
                r = r.left;
            }
        }
        return null;
    }

    /**
     * Returns a key-value mapping associated with the least key in this map, or
     * null if the map is empty.
     * 
     * @return The lowest key in the map
     */
    public TreeEntry<K, V> firstEntry() {
        TreeEntry<K, V> r = root;
        while (r != null) {
            if (r.left == null) {
                break;
            }
            r = r.left;
        }

        return r;
    }

    /**
     * Returns a key-value mapping associated with the greatest key in this map,
     * or null if the map is empty.
     * 
     * @return The entry associated with the greates key in the map.
     */
    public TreeEntry<K, V> lastEntry() {
        TreeEntry<K, V> r = root;
        while (r != null) {
            if (r.right == null) {
                break;
            }
            r = r.right;
        }

        return r;
    }

    /**
     * Returns a key-value mapping associated with the greatest key strictly
     * less than the given key, or null if there is no such key
     * 
     * @param key
     *            the key.
     * @return
     */
    public TreeEntry<K, V> lowerEntry(K key) {
        TreeEntry<K, V> n = root;
        TreeEntry<K, V> l = null;
        while (n != null) {
            int c = compare(key, n.key);
            if (c <= 0) {
                n = n.left;
            } else {
                //Update the low node:
                if (l == null || compare(n.key, l.key) > 0) {
                    l = n;
                }
                //Now need to scan the right tree to see if there is
                //a higher low value to consider:
                if (n.right == null) {
                    break;
                }
                n = n.right;
            }
        }
        return l;
    }

    /**
     * Returns a key-value mapping associated with the greatest key less than or
     * equal to the given key, or null if there is no such key.
     * 
     * @param key
     *            The key for which to search.
     * @return a key-value mapping associated with the greatest key less than or
     *         equal to the given key, or null if there is no such key.
     */
    public TreeEntry<K, V> floorEntry(K key) {
        TreeEntry<K, V> n = root;
        TreeEntry<K, V> l = null;
        while (n != null) {
            int c = compare(key, n.key);
            if (c == 0) {
                return n;
            }

            if (c < 0) {
                n = n.left;
            } else {
                //Update the low node:
                if (l == null || compare(n.key, l.key) > 0) {
                    l = n;
                }
                //Now need to scan the right tree to see if there is
                //a higher low value to consider:
                if (n.right == null) {
                    break;
                }
                n = n.right;
            }
        }
        return l;
    }

    /**
     * Returns a key-value mapping associated with the lowest key strictly
     * greater than the given key, or null if there is no such key
     * 
     * @param key
     *            The key
     * @return a key-value mapping associated with the lowest key strictly
     *         greater than the given key
     */
    public TreeEntry<K, V> upperEntry(K key) {
        TreeEntry<K, V> n = root;
        TreeEntry<K, V> h = null;
        while (n != null) {
            int c = compare(key, n.key);
            if (c >= 0) {
                n = n.right;
            } else {
                //Update the high node:
                if (h == null || compare(n.key, h.key) < 0) {
                    h = n;
                }
                //Now need to scan the left tree to see if there is
                //a lower high value to consider:
                if (n.left == null) {
                    break;
                }
                n = n.left;
            }
        }
        return h;
    }

    /**
     * Returns a key-value mapping associated with the least key greater than or
     * equal to the given key, or null if there is no such key.
     * 
     * @param key
     * @return
     */
    public TreeEntry<K, V> ceilingEntry(K key) {
        TreeEntry<K, V> n = root;
        TreeEntry<K, V> h = null;
        while (n != null) {
            int c = compare(key, n.key);
            if (c == 0) {
                return n;
            }

            if (c > 0) {
                n = n.right;
            } else {
                //Update the high node:
                if (h == null || compare(n.key, h.key) < 0) {
                    h = n;
                }
                //Now need to scan the left tree to see if there is
                //a lower high value to consider:
                if (n.left == null) {
                    break;
                }
                n = n.left;
            }
        }
        return h;
    }

    static private final <K,V> TreeEntry<K, V> next(TreeEntry<K, V> n) {
        if (n == null)
            return null;
        else if (n.right != null) {
            TreeEntry<K, V> p = n.right;
            while (p.left != null) {
                p = p.left;
            }
            return p;
        } else {
            TreeEntry<K, V> p = n.parent;
            TreeEntry<K, V> ch = n;
            while (p != null && ch == p.right) {
                ch = p;
                p = p.parent;
            }
            return p;
        }
    }
    
    static private final <K,V> TreeEntry<K, V> previous(TreeEntry<K, V> n) {
        if (n == null)
            return null;
        else if (n.left != null) {
            TreeEntry<K, V> p = n.left;
            while (p.right != null) {
                p = p.right;
            }
            return p;
        } else {
            TreeEntry<K, V> p = n.parent;
            TreeEntry<K, V> ch = n;
            while (p != null && ch == p.left) {
                ch = p;
                p = p.parent;
            }
            return p;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#keySet()
     */
    public Set<K> keySet() {
        return new AbstractSet<K>() {

            @Override
            public void clear() {
                TreeMap.this.clear();
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean remove(Object o) {
                return TreeMap.this.remove((K) o) != null;
            }

            @Override
            public Iterator<K> iterator() {
                return new KeyIterator();
            }

            @Override
            public int size() {
                return count;
            }
        };

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map<? extends K, ? extends V> t) {
        for (Map.Entry<? extends K, ? extends V> entry : t.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#remove(java.lang.Object)
     */
    public V remove(K key) {
        return removeEntry(getEntry(key, root));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public V put(final K key, final V value) {

        if (root == null) {
            // map is empty
            root = new TreeEntry<K, V>(key, value, null, this);
            count++;
            return null;
        }
        TreeEntry<K, V> n = root;

        // add new mapping
        while (true) {
            int c = compare(key, n.key);

            if (c == 0) {
                V old = n.value;
                n.value = value;
                return old;
            } else if (c < 0) {
                if (n.left != null) {
                    n = n.left;
                } else {
                    n.left = new TreeEntry<K, V>(key, value, n, this);
                    count++;
                    doRedBlackInsert(n.left);
                    return null;
                }
            } else { // c > 0
                if (n.right != null) {
                    n = n.right;
                } else {
                    n.right = new TreeEntry<K, V>(key, value, n, this);
                    count++;
                    doRedBlackInsert(n.right);
                    return null;
                }
            }
        }
    }

    /**
     * complicated red-black insert stuff. Based on apache commons BidiMap
     * method:
     * 
     * @param n
     *            the newly inserted node
     */
    private void doRedBlackInsert(final TreeEntry<K, V> n) {
        TreeEntry<K, V> currentNode = n;
        color(currentNode, RED);

        while (currentNode != null && currentNode != root && isRed(currentNode.parent)) {
            if (isLeftChild(parent(currentNode))) {
                TreeEntry<K, V> y = getRight(getGrandParent(currentNode));

                if (isRed(y)) {
                    color(parent(currentNode), BLACK);
                    color(y, BLACK);
                    color(getGrandParent(currentNode), RED);

                    currentNode = getGrandParent(currentNode);
                } else {
                    if (isRightChild(currentNode)) {
                        currentNode = parent(currentNode);

                        rotateLeft(currentNode);
                    }

                    color(parent(currentNode), BLACK);
                    color(getGrandParent(currentNode), RED);

                    if (getGrandParent(currentNode) != null) {
                        rotateRight(getGrandParent(currentNode));
                    }
                }
            } else {

                // just like clause above, except swap left for right
                TreeEntry<K, V> y = getLeft(getGrandParent(currentNode));

                if (isRed(y)) {
                    color(parent(currentNode), BLACK);
                    color(y, BLACK);
                    color(getGrandParent(currentNode), RED);

                    currentNode = getGrandParent(currentNode);
                } else {
                    if (isLeftChild(currentNode)) {
                        currentNode = parent(currentNode);

                        rotateRight(currentNode);
                    }

                    color(parent(currentNode), BLACK);
                    color(getGrandParent(currentNode), RED);

                    if (getGrandParent(currentNode) != null) {
                        rotateLeft(getGrandParent(currentNode));
                    }
                }
            }
        }

        color(root, BLACK);
    }

    //Based on Apache common's TreeBidiMap
    private void rotateLeft(TreeEntry<K, V> n) {
        TreeEntry<K, V> r = n.right;
        n.right = r.left;
        if (r.left != null) {
            r.left.parent = n;
        }
        r.parent = n.parent;
        if (n.parent == null) {
            root = r;
        } else if (n.parent.left == n) {
            n.parent.left = r;
        } else {
            n.parent.right = r;
        }

        r.left = n;
        n.parent = r;
    }

    //Based on Apache common's TreeBidiMap    
    private void rotateRight(TreeEntry<K, V> n) {
        TreeEntry<K, V> l = n.left;
        n.left = l.right;
        if (l.right != null) {
            l.right.parent = n;
        }
        l.parent = n.parent;
        if (n.parent == null) {
            root = l;
        } else if (n.parent.right == n) {
            n.parent.right = l;
        } else {
            n.parent.left = l;
        }
        l.right = n;
        n.parent = l;
    }

    /**
     * complicated red-black delete stuff. Based on Apache Common's TreeBidiMap
     * 
     * @param n
     *            the node to be deleted
     */
    public final V removeEntry(TreeEntry<K, V> n) {
        if (n == null) {
            return null;
        }

        if (n.map != this) {
            throw new IllegalStateException("Node not in list");
        }

        V old = n.value;

        count--;

        //if deleted node has both left and children, swap with
        // the next greater node
        if (n.left != null && n.right != null) {
            TreeEntry<K, V> next = next(n);
            n.key = next.key;
            n.value = next.value;
            n = next;
        }

        TreeEntry<K, V> replacement = n.left != null ? n.left : n.right;

        if (replacement != null) {
            replacement.parent = n.parent;

            if (n.parent == null) {
                root = replacement;
            } else if (n == n.parent.left) {
                n.parent.left = replacement;
            } else {
                n.parent.right = replacement;
            }

            n.left = null;
            n.right = null;
            n.parent = null;

            if (isBlack(n)) {
                doRedBlackDeleteFixup(replacement);
            }
        } else {

            // replacement is null
            if (n.parent == null) {
                // empty tree
                root = null;
            } else {

                // deleted node had no children
                if (isBlack(n)) {
                    doRedBlackDeleteFixup(n);
                }

                if (n.parent != null) {
                    if (n == n.parent.left) {
                        n.parent.left = null;
                    } else {
                        n.parent.right = null;
                    }

                    n.parent = null;
                }
            }
        }
        return old;
    }

    /**
     * complicated red-black delete stuff. Based on Apache Commons TreeBidiMap.
     * 
     * @param replacementNode
     *            the node being replaced
     */
    private void doRedBlackDeleteFixup(final TreeEntry<K, V> replacementNode) {
        TreeEntry<K, V> currentNode = replacementNode;

        while (currentNode != root && isBlack(currentNode)) {
            if (isLeftChild(currentNode)) {
                TreeEntry<K, V> siblingNode = getRight(parent(currentNode));

                if (isRed(siblingNode)) {
                    color(siblingNode, BLACK);
                    color(parent(currentNode), RED);
                    rotateLeft(parent(currentNode));

                    siblingNode = getRight(parent(currentNode));
                }

                if (isBlack(getLeft(siblingNode)) && isBlack(getRight(siblingNode))) {
                    color(siblingNode, RED);

                    currentNode = parent(currentNode);
                } else {
                    if (isBlack(getRight(siblingNode))) {
                        color(getLeft(siblingNode), BLACK);
                        color(siblingNode, RED);
                        rotateRight(siblingNode);

                        siblingNode = getRight(parent(currentNode));
                    }

                    color(siblingNode, getColor(parent(currentNode)));
                    color(parent(currentNode), BLACK);
                    color(getRight(siblingNode), BLACK);
                    rotateLeft(parent(currentNode));

                    currentNode = root;
                }
            } else {
                TreeEntry<K, V> siblingNode = getLeft(parent(currentNode));

                if (isRed(siblingNode)) {
                    color(siblingNode, BLACK);
                    color(parent(currentNode), RED);
                    rotateRight(parent(currentNode));

                    siblingNode = getLeft(parent(currentNode));
                }

                if (isBlack(getRight(siblingNode)) && isBlack(getLeft(siblingNode))) {
                    color(siblingNode, RED);

                    currentNode = parent(currentNode);
                } else {
                    if (isBlack(getLeft(siblingNode))) {
                        color(getRight(siblingNode), BLACK);
                        color(siblingNode, RED);
                        rotateLeft(siblingNode);

                        siblingNode = getLeft(parent(currentNode));
                    }

                    color(siblingNode, getColor(parent(currentNode)));
                    color(parent(currentNode), BLACK);
                    color(getLeft(siblingNode), BLACK);
                    rotateRight(parent(currentNode));

                    currentNode = root;
                }
            }
        }

        color(currentNode, BLACK);
    }


    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#size()
     */
    public int size() {
        return count;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#values()
     */
    public Collection<V> values() {
        return new AbstractCollection<V>() {

            @Override
            public Iterator<V> iterator() {
                return new ValueIterator();
            }

            @Override
            public int size() {
                return count;
            }
        };
    }

    private static <K, V> TreeEntry<K, V> parent(TreeEntry<K, V> n) {
        return (n == null ? null : n.parent);
    }

    private static <K, V> void color(TreeEntry<K, V> n, boolean c) {
        if (n != null)
            n.color = c;
    }

    private static <K, V> boolean getColor(TreeEntry<K, V> n) {
        return (n == null ? BLACK : n.color);
    }

    /**
     * get a node's left child. mind you, the node may not exist. no problem
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> TreeEntry<K, V> getLeft(TreeEntry<K, V> n) {
        return (n == null) ? null : n.left;
    }

    /**
     * get a node's right child. mind you, the node may not exist. no problem
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> TreeEntry<K, V> getRight(TreeEntry<K, V> n) {
        return (n == null) ? null : n.right;
    }

    /**
     * is the specified node red? if the node does not exist, no, it's black,
     * thank you
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> boolean isRed(TreeEntry<K, V> n) {
        return n == null ? false : n.color == RED;
    }

    /**
     * is the specified black red? if the node does not exist, sure, it's black,
     * thank you
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> boolean isBlack(final TreeEntry<K, V> n) {
        return n == null ? true : n.color == BLACK;
    }

    /**
     * is this node its parent's left child? mind you, the node, or its parent,
     * may not exist. no problem. if the node doesn't exist ... it's its
     * non-existent parent's left child. If the node does exist but has no
     * parent ... no, we're not the non-existent parent's left child. Otherwise
     * (both the specified node AND its parent exist), check.
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> boolean isLeftChild(final TreeEntry<K, V> node) {

        return node == null ? true : (node.parent == null ? false : (node == node.parent.left));
    }

    /**
     * is this node its parent's right child? mind you, the node, or its parent,
     * may not exist. no problem. if the node doesn't exist ... it's its
     * non-existent parent's right child. If the node does exist but has no
     * parent ... no, we're not the non-existent parent's right child. Otherwise
     * (both the specified node AND its parent exist), check.
     * 
     * @param node
     *            the node (may be null) in question
     * @param index
     *            the KEY or VALUE int
     */
    private static <K, V> boolean isRightChild(final TreeEntry<K, V> node) {
        return node == null ? true : (node.parent == null ? false : (node == node.parent.right));

    }

    /**
     * get a node's grandparent. mind you, the node, its parent, or its
     * grandparent may not exist. no problem
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> TreeEntry<K, V> getGrandParent(final TreeEntry<K, V> node) {
        return parent(parent(node));
    }

    public static class TreeEntry<K, V> implements Map.Entry<K, V>, Serializable {

        private static final long serialVersionUID = 8490652911043012737L;
        
        TreeMap<K, V> map;
        V value;
        K key;
        boolean color = BLACK;

        TreeEntry<K, V> parent;
        TreeEntry<K, V> left;
        TreeEntry<K, V> right;

        TreeEntry(K key, V val, TreeEntry<K, V> parent, TreeMap<K, V> map) {
            this.key = key;
            this.parent = parent;
            this.value = val;
            this.map = map;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Map.Entry#getKey()
         */
        public K getKey() {
            return key;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Map.Entry#getValue()
         */
        public V getValue() {
            return value;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Map.Entry#setValue(java.lang.Object)
         */
        public V setValue(V val) {
            V old = this.value;
            this.value = val;
            return old;
        }

        @SuppressWarnings("unchecked")
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry e = (Map.Entry) o;

            return (key == null ? e.getKey() == null : key.equals(e.getKey())) && (value == null ? e.getValue() == null : value.equals(e.getValue()));
        }

        public int hashCode() {
            int keyHash = (key == null ? 0 : key.hashCode());
            int valueHash = (value == null ? 0 : value.hashCode());
            return keyHash ^ valueHash;
        }
        
        public TreeEntry<K, V> next() {
            return TreeMap.next(this);
        }
        
        public TreeEntry<K, V> previous() {
            return TreeMap.previous(this);
        }
        
        @Override
        public String toString() {
            return "{ key: " +key+", value: "+value+" }";
        }
    }

    private class ValueIterator extends AbstractEntryIterator<V> {

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        public V next() {
            getNext();
            if (last == null) {
                return null;
            } else {
                return last.value;
            }
        }
    }

    private class KeyIterator extends AbstractEntryIterator<K> {

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        public K next() {
            getNext();
            if (last == null) {
                return null;
            } else {
                return last.key;
            }
        }
    }

    private class EntryIterator extends AbstractEntryIterator<Map.Entry<K, V>> {
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        public Entry<K, V> next() {
            getNext();
            if (last == null) {
                return null;
            } else {
                return last;
            }
        }

    }

    private abstract class AbstractEntryIterator<T> implements Iterator<T> {

        TreeEntry<K, V> last = null;
        TreeEntry<K, V> next = firstEntry();

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#hasNext()
         */
        public boolean hasNext() {
            return next != null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        protected TreeEntry<K, V> getNext() {
            last = next;
            next = TreeMap.next(next);
            return last;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#remove()
         */
        public void remove() {
            TreeMap.this.removeEntry(last);
            last = null;
        }

    }
}