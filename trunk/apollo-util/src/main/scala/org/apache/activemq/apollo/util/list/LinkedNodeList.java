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
import java.util.Iterator;

/**
 * Provides a list of LinkedNode objects. 
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LinkedNodeList<T extends LinkedNode<T>> implements Iterable<T> {

    T head;
    int size;

    public LinkedNodeList() {
    }

    final public boolean isEmpty() {
        return head == null;
    }

    final public void addLast(T node) {
        node.linkToTail(this);
    }

    final public void addFirst(T node) {
        node.linkToHead(this);
    }

    final public T getHead() {
        return head;
    }

    final public T getTail() {
        if( head==null ) {
            return null;
        }
        return head.prev;
    }
    
    final public void clear() {
        while (head != null) {
            head.unlink();
        }
    }

    final public void addLast(LinkedNodeList<T> list) {
        if (list.isEmpty()) {
            return;
        }
        if (head == null) {
            head = list.head;
            reparent(list);
        } else {
            getTail().linkAfter(list);
        }
    }

    final public void addFirst(LinkedNodeList<T> list) {
        if (list.isEmpty()) {
            return;
        }
        if (head == null) {
            reparent(list);
            head = list.head;
            list.head = null;
        } else {
            getHead().linkBefore(list);
        }
    }

    final public T reparent(LinkedNodeList<T> list) {
        size += list.size;
        T n = list.head;
        do {
            n.list = this;
            n = n.next;
        } while (n != list.head);
        list.head = null;
        list.size = 0;
        return n;
    }

    /**
     * Move the head to the tail and returns the new head node.
     * 
     * @return
     */
    final public T rotate() {
    	if( head ==null )
    		return null;
        return head = head.getNextCircular();
    }

    /**
     * Move the head to the tail and returns the new head node.
     * 
     * @return
     */
    final public void rotateTo(T head) {
    	assert head!=null: "Cannot rotate to a null head";
    	assert head.list == this : "Cannot rotate to a node not linked to this list";
        this.head = head;
    }

    public int size() {
        return size;
    }

    @Override
    final public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first=true;
        T cur = getHead();
        while( cur!=null ) {
            if( !first ) {
                sb.append(", ");
            }
            sb.append(cur);
            first=false;
            cur = cur.getNext();
        }
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Copies the nodes of the LinkedNodeList to an ArrayList.
     * @return
     */
    final public ArrayList<T> toArrayList() {
    	ArrayList<T> rc = new ArrayList<T>(size);
    	T cur = head;
    	while( cur!=null ) {
    		rc.add(cur);
    		cur = cur.getNext();
    	}
    	return rc;
    }

    /**
     * Copies the nodes of the LinkedNodeList to an ArrayList in reverse order.
     * @return
     */
    final public ArrayList<T> toArrayListReversed() {
        ArrayList<T> rc = new ArrayList<T>(size);
        if( head != null ) {
            T cur = getTail();;
            while( cur!=null ) {
                rc.add(cur);
                cur = cur.getPrevious();
            }
        }
        return rc;
    }


    /**
     * Copies the nodes of the LinkedNodeList to the specified array.
     * @return the passed array.
     */
    final public T[] toArray(T[] array) {
        int pos = 0;
    	ArrayList<T> rc = new ArrayList<T>(size);
    	T cur = head;
    	while( cur!=null ) {
    		array[pos] = cur;
            pos ++;
    		cur = cur.getNext();
    	}
        return array;
    }
    
    final public Iterator<T> iterator() {
        return new Iterator<T>() {
            T next = getHead();
            private T last;
            
            public boolean hasNext() {
                return next!=null;
            }

            public T next() {
                last = next;
                next = last.getNext();
                return last;
            }

            public void remove() {
                if( last==null ) {
                    throw new IllegalStateException();
                }
                last.unlink();
                last=null;
            }
        };
    }
}
