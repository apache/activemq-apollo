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
package org.apache.activemq.util;

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
 */
public class TreeMap<K, V> {
    private static final boolean RED = false;
    private static final boolean BLACK = true;

    private int count;
    private TreeMapNode<K, V> root;
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
        TreeMapNode<K, V> first = firstNode();
        if (first != null) {
            return first.key;
        }
        return null;
    }

    private TreeMapNode<K, V> firstNode() {
        if (root == null) {
            return null;
        }
        TreeMapNode<K, V> leftMost = root;
        while (leftMost.left != null) {
            leftMost = leftMost.left;
        }

        return leftMost;
    }

    /**
     * @return The last key in the map.
     */
    public K lastKey() {
        TreeMapNode<K, V> last = lastNode();
        if (last != null) {
            return last.key;
        }
        return null;
    }

    private TreeMapNode<K, V> lastNode() {
        if (root == null) {
            return null;
        }
        TreeMapNode<K, V> rightMost = root;
        while (rightMost.right != null) {
            rightMost = rightMost.left;
        }

        return rightMost;
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
        return findInternal(key, root) != null;
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
                    TreeMapNode<K, V> ours = findInternal(entry.getKey(), root);
                    if (ours != null) {
                        return ours.val == null ? entry.getValue() == null : ours.val.equals(entry.getValue());
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
                return TreeMap.this.removeNode((TreeMapNode<K, V>) o) != null;
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
        TreeMapNode<K, V> node = findInternal(key, root);
        if (node != null) {
            return node.val;
        }
        return null;
    }

    private final TreeMapNode<K, V> findInternal(K key, TreeMapNode<K, V> r) {
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
    public Entry<K, V> firstEntry() {
        TreeMapNode<K, V> r = root;
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
    public Entry<K, V> lastEntry() {
        TreeMapNode<K, V> r = root;
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
    public Entry<K, V> lowerEntry(K key) {
        TreeMapNode<K, V> n = root;
        TreeMapNode<K, V> l = null;
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
    public Entry<K, V> floorEntry(K key) {
        TreeMapNode<K, V> n = root;
        TreeMapNode<K, V> l = null;
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
    public Entry<K, V> upperEntry(K key) {
        TreeMapNode<K, V> n = root;
        TreeMapNode<K, V> h = null;
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
    public Entry<K, V> ceilingEntry(K key) {
        TreeMapNode<K, V> n = root;
        TreeMapNode<K, V> h = null;
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

    protected final TreeMapNode<K, V> findNext(TreeMapNode<K, V> n) {
        if (n == null)
            return null;
        else if (n.right != null) {
            TreeMapNode<K, V> p = n.right;
            while (p.left != null) {
                p = p.left;
            }
            return p;
        } else {
            TreeMapNode<K, V> p = n.parent;
            TreeMapNode<K, V> ch = n;
            while (p != null && ch == p.right) {
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
        return removeNode(findInternal(key, root));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public V put(final K key, final V value) {

        if (root == null) {
            // map is empty
            root = new TreeMapNode<K, V>(key, value, null, this);
            count++;
            return null;
        }
        TreeMapNode<K, V> n = root;

        // add new mapping
        while (true) {
            int c = compare(key, n.key);

            if (c == 0) {
                V old = n.val;
                n.val = value;
                return old;
            } else if (c < 0) {
                if (n.left != null) {
                    n = n.left;
                } else {
                    n.left = new TreeMapNode<K, V>(key, value, n, this);
                    count++;
                    doRedBlackInsert(n.left);
                    return null;
                }
            } else { // c > 0
                if (n.right != null) {
                    n = n.right;
                } else {
                    n.right = new TreeMapNode<K, V>(key, value, n, this);
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
    private void doRedBlackInsert(final TreeMapNode<K, V> n) {
        TreeMapNode<K, V> currentNode = n;
        color(currentNode, RED);

        while (currentNode != null && currentNode != root && isRed(currentNode.parent)) {
            if (isLeftChild(parent(currentNode))) {
                TreeMapNode<K, V> y = getRight(getGrandParent(currentNode));

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
                TreeMapNode<K, V> y = getLeft(getGrandParent(currentNode));

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
    private void rotateLeft(TreeMapNode<K, V> n) {
        TreeMapNode<K, V> r = n.right;
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
    private void rotateRight(TreeMapNode<K, V> n) {
        TreeMapNode<K, V> l = n.left;
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
    private final V removeNode(TreeMapNode<K, V> n) {
        if (n == null) {
            return null;
        }

        if (n.map != this) {
            throw new IllegalStateException("Node not in list");
        }

        V old = n.val;

        count--;

        //if deleted node has both left and children, swap with
        // the next greater node
        if (n.left != null && n.right != null) {
            TreeMapNode<K, V> next = findNext(n);
            n.key = next.key;
            n.val = next.val;
            n = next;
        }

        TreeMapNode<K, V> replacement = n.left != null ? n.left : n.right;

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
    private void doRedBlackDeleteFixup(final TreeMapNode<K, V> replacementNode) {
        TreeMapNode<K, V> currentNode = replacementNode;

        while (currentNode != root && isBlack(currentNode)) {
            if (isLeftChild(currentNode)) {
                TreeMapNode<K, V> siblingNode = getRight(currentNode.parent);

                if (isRed(siblingNode)) {
                    color(siblingNode, BLACK);
                    color(parent(currentNode), RED);
                    rotateLeft(parent(currentNode));

                    siblingNode = getRight(parent(currentNode));
                }

                if (isBlack(getLeft(siblingNode)) && isBlack(getRight(siblingNode))) {
                    color(siblingNode, RED);

                    currentNode = currentNode.parent;
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
                TreeMapNode<K, V> siblingNode = getRight(currentNode);

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

    private static <K, V> TreeMapNode<K, V> parent(TreeMapNode<K, V> n) {
        return (n == null ? null : n.parent);
    }

    private static <K, V> void color(TreeMapNode<K, V> n, boolean c) {
        if (n != null)
            n.color = c;
    }

    private static <K, V> boolean getColor(TreeMapNode<K, V> n) {
        return (n == null ? BLACK : n.color);
    }

    /**
     * get a node's left child. mind you, the node may not exist. no problem
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> TreeMapNode<K, V> getLeft(TreeMapNode<K, V> n) {
        return (n == null) ? null : n.left;
    }

    /**
     * get a node's right child. mind you, the node may not exist. no problem
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> TreeMapNode<K, V> getRight(TreeMapNode<K, V> n) {
        return (n == null) ? null : n.right;
    }

    /**
     * is the specified node red? if the node does not exist, no, it's black,
     * thank you
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> boolean isRed(TreeMapNode<K, V> n) {
        return n == null ? false : n.color == RED;
    }

    /**
     * is the specified black red? if the node does not exist, sure, it's black,
     * thank you
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> boolean isBlack(final TreeMapNode<K, V> n) {
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
    private static <K, V> boolean isLeftChild(final TreeMapNode<K, V> node) {

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
    private static <K, V> boolean isRightChild(final TreeMapNode<K, V> node) {
        return node == null ? true : (node.parent == null ? false : (node == node.parent.right));

    }

    /**
     * get a node's grandparent. mind you, the node, its parent, or its
     * grandparent may not exist. no problem
     * 
     * @param node
     *            the node (may be null) in question
     */
    private static <K, V> TreeMapNode<K, V> getGrandParent(final TreeMapNode<K, V> node) {
        return parent(parent(node));
    }

    public static class TreeMapNode<K, V> implements Map.Entry<K, V> {
        TreeMap<K, V> map;
        V val;
        K key;
        boolean color = BLACK;

        TreeMapNode<K, V> parent;
        TreeMapNode<K, V> left;
        TreeMapNode<K, V> right;

        TreeMapNode(K key, V val, TreeMapNode<K, V> parent, TreeMap<K, V> map) {
            this.key = key;
            this.parent = parent;
            this.val = val;
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
            return val;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Map.Entry#setValue(java.lang.Object)
         */
        public V setValue(V val) {
            V old = this.val;
            this.val = val;
            return old;
        }

        @SuppressWarnings("unchecked")
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry e = (Map.Entry) o;

            return (key == null ? e.getKey() == null : key.equals(e.getKey())) && (val == null ? e.getValue() == null : val.equals(e.getValue()));
        }

        public int hashCode() {
            int keyHash = (key == null ? 0 : key.hashCode());
            int valueHash = (val == null ? 0 : val.hashCode());
            return keyHash ^ valueHash;
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
                return last.val;
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

        TreeMapNode<K, V> last = null;
        TreeMapNode<K, V> next = firstNode();

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
        protected TreeMapNode<K, V> getNext() {
            last = next;
            next = findNext(next);
            return last;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#remove()
         */
        public void remove() {
            TreeMap.this.removeNode(last);
            last = null;
        }

    }
}