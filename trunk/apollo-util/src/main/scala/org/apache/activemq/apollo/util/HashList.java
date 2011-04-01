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

import java.util.HashMap;
import java.util.Iterator;

import org.apache.activemq.apollo.util.list.LinkedNode;
import org.apache.activemq.apollo.util.list.LinkedNodeList;

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