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
package org.apache.activemq.apollo.util.list;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.activemq.apollo.util.Comparators;
import org.apache.activemq.apollo.util.TreeMap;

public class SortedLinkedList<T extends SortedLinkedListNode<T>> {

    protected final TreeMap<Long, T> index;
    T head;
    int size;
   
    public SortedLinkedList() {
        index = new TreeMap<Long, T>(Comparators.LONG_COMPARATOR);
    }

    public boolean isEmpty() {
        return head == null;
    }

    public void add(T node) {
        T prev = null;
        if (head != null) {
            // Optimize for case where this is at tail:
            if (head.prev.getSequence() < node.getSequence()) {
                prev = head.prev;
            } else {
                Entry<Long, T> entry = index.lowerEntry(node.getSequence());
                if (entry != null) {
                    prev = entry.getValue();
                }
            }
        }
        // T prev = index.lower(node);
        // If this the lowest then the new head is this.
        if (prev == null) {
            node.linkToHead(this);
        } else {
            prev.linkAfter(node);
        }
    }
    
    /**
     * @param sequence The sequence number of the element to get.
     */
    public T get(long sequence) {
        return index.get(sequence);
    }

    public T lower(long sequence, boolean inclusive) {
        Entry<Long, T> lower = index.floorEntry(sequence);
        if (lower == null) {
            return null;
        }

        if (inclusive) {
            return lower.getValue();
        }

        if (lower.getKey() == sequence) {
            return lower.getValue().prev;
        } else {
            return lower.getValue();
        }

    }

    public T upper(long sequence, boolean inclusive) {
        if (head == null || head.prev.getSequence() < sequence) {
            return null;
        }

        Entry<Long, T> upper = index.ceilingEntry(sequence);
        if (upper == null) {
            return null;
        }

        if (inclusive) {
            return upper.getValue();
        }

        if (upper.getKey() == sequence) {
            return upper.getValue().next;
        } else {
            return upper.getValue();
        }
    }

    public void remove(T node) {
        if (node.list == this) {
            node.unlink();
        }
    }

    public T getHead() {
        return head;
    }

    public T getTail() {
        return head.prev;
    }

    public void clear() {
        while (head != null) {
            head.unlink();
        }
        index.clear();
    }

    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        T cur = getHead();
        while (cur != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(cur);
            first = false;
            cur = cur.getNext();
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Copies the nodes of the LinkedNodeList to an ArrayList.
     * 
     * @return
     */
    public ArrayList<T> toArrayList() {
        ArrayList<T> rc = new ArrayList<T>(size);
        T cur = head;
        while (cur != null) {
            rc.add(cur);
            cur = cur.getNext();
        }
        return rc;
    }

    
}
