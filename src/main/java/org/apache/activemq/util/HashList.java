package org.apache.activemq.util;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.kahadb.util.LinkedNodeList;
import org.apache.kahadb.util.LinkedNode;

/**
 * <p>
 * Title: Sonic MQ v6.1
 * </p>
 * <p>
 * Description: Sonic MQ v6.1
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company: Sonic Software Corporation
 * </p>
 * 
 * @author Colin MacNaughton
 * @version 6.1
 */

public class HashList<E> {
    private HashMap<E, HashListNode> m_index = null;
    private LinkedNodeList<HashListNode> m_list = null;

    public HashList() {
        m_index = new HashMap<E, HashListNode>();
        m_list = new LinkedNodeList<HashListNode>();
    }

    /**
     * Adds an object to the list if it is not already present.
     * 
     * @param o
     *            True if the object was added.
     */
    public final boolean add(E o) {
        HashListNode n = (HashListNode) m_index.get(o);
        if (n == null) {
            n = new HashListNode(o);
            m_index.put(o, n);
            return true;
        } else {
            return false;
        }
    }

    public final boolean remove(E o) {
        HashListNode n = m_index.remove(o);
        if (n != null) {
            n.unlink();
            return true;
        }
        return false;
    }

    public final Object get(E o) {
        HashListNode n = m_index.get(o);
        if (n == null) {
            return null;
        } else
            return n.elem;
    }

    public final int size() {
        return m_index.size();
    }

    public final boolean contains(Object o) {
        return m_index.containsKey(o);
    }

    /**
     * @return Returns a head to tail iterator of the underlying list.
     */
    public final Iterator<E> iterator() {
        return new Iterator<E>() {
            HashListNode next = m_list.getHead();

            public void remove() {
                HashListNode newNext = next.getNext();
                m_index.remove(next.elem);
                next.unlink();
                next = newNext;
            }

            public boolean hasNext() {
                return next != null;
            }

            public E next() {
                try {
                    return next.elem;
                } finally {
                    next = next.getNext();
                }
            }
        };
    }

    private class HashListNode extends LinkedNode<HashListNode> {
        private final E elem;

        HashListNode(E elem) {
            this.elem = elem;
        }

        public int hashCode() {
            return elem.hashCode();
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            
            if (o == null || o.hashCode() != hashCode()) {
                return false;
            } else {
                HashListNode node = null;
                if(getClass().isInstance(o))
                {
                    node = getClass().cast(o);
                }
                if (node == null) {
                    return false;
                }
                return equals(node);
            }
        }

        public boolean equals(HashList<E>.HashListNode node) {
            return node.elem.equals(elem);
        }
    }

}