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

import java.util.ArrayList;

import org.apache.activemq.apollo.util.list.LinkedNode;
import org.apache.activemq.apollo.util.list.LinkedNodeList;

public class PriorityLinkedList<E extends LinkedNode<E>> {

    private Mapper<Integer, E> priorityMapper;
    private final ArrayList<LinkedNodeList<E>> priorityLists;
    private int highesPriority = 0;

    public PriorityLinkedList(int numPriorities) {
        this(numPriorities, null);
    }

    public PriorityLinkedList(int numPriorities, Mapper<Integer, E> priorityMapper) {
        this.priorityMapper = priorityMapper;
        priorityLists = new ArrayList<LinkedNodeList<E>>();
        for (int i = 0; i <= numPriorities; i++) {
            priorityLists.add(new LinkedNodeList<E>());
        }
    }

    public final int getHighestPriority() {
        return highesPriority;
    }

    /**
     * Gets the element at the front of the list:
     * 
     * @return
     */
    public final E poll() {
        LinkedNodeList<E> ll = getHighestPriorityList();
        if (ll == null) {
            return null;
        }
        E node = ll.getHead();
        node.unlink();

        return node;
    }

    public final boolean isEmpty() {
        return peek() != null;
    }

    /**
     * Gets the element at the front of the list:
     * 
     * @return
     */
    public final E peek() {
        LinkedNodeList<E> ll = getHighestPriorityList();
        if (ll == null) {
            return null;
        }

        return ll.getHead();
    }

    public final void add(E element) {
        int prio = priorityMapper.map(element);
        add(element, prio);
    }

    public final void add(E element, int prio) {
        LinkedNodeList<E> ll = priorityLists.get(prio);
        ll.addLast(element);
        if (prio > highesPriority) {
            highesPriority = prio;
        }
    }

    private final LinkedNodeList<E> getHighestPriorityList() {
        LinkedNodeList<E> ll = priorityLists.get(highesPriority);
        while (ll.isEmpty()) {
            if (highesPriority == 0) {
                return null;
            }
            highesPriority--;
            ll = priorityLists.get(highesPriority);
        }

        return ll;
    }

    public Mapper<Integer, E> getPriorityMapper() {
        return priorityMapper;
    }

    public void setPriorityMapper(Mapper<Integer, E> priorityMapper) {
        this.priorityMapper = priorityMapper;
    }

}