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

public abstract class SortedLinkedListNode<T extends SortedLinkedListNode<T>> {

    protected SortedLinkedList<T> list;
    protected T next;
    protected T prev;

    public SortedLinkedListNode() {
    }

    @SuppressWarnings("unchecked")
    private T getThis() {
        return (T) this;
    }

    public T getHeadNode() {
        return list.head;
    }

    public T getTailNode() {
        return list.head.prev;
    }

    public T getNext() {
        return isTailNode() ? null : next;
    }

    public T getPrevious() {
        return isHeadNode() ? null : prev;
    }

    public T getNextCircular() {
        return next;
    }

    public T getPreviousCircular() {
        return prev;
    }

    public boolean isHeadNode() {
        return list.head == this;
    }

    public boolean isTailNode() {
        return list.head.prev == this;
    }

    /**
     * Removes this node out of the linked list it is chained in.
     */
    public boolean unlink() {

        // If we are already unlinked...
        if (list == null) {
            return false;
        }

        if (getThis() == prev) {
            // We are the only item in the list
            list.head = null;
        } else {
            // given we linked prev<->this<->next
            next.prev = prev; // prev<-next
            prev.next = next; // prev->next

            if (isHeadNode()) {
                list.head = next;
            }
        }
        list.index.remove(this.getSequence());
        list.size--;
        list = null;
        return true;
    }

    private void addToIndex(T node, SortedLinkedList<T> list) {
        if (node.list != null) {
            throw new IllegalArgumentException("You only insert nodes that are not in a list");
        }

        T old = list.index.put(node.getSequence(), node);
        if(old != null)
        {
            list.index.put(old.getSequence(), old);
            throw new IllegalArgumentException("A node with this key is already in the list");
        }

        node.list = list;
        list.size++;
    }

    private final void checkLinkOk(T toLink) {
        if (toLink == this) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }

        if (list == null) {
            throw new IllegalArgumentException("This node is not yet in a list");
        }
    }

    /**
     * Adds the specified node to the head of the list.
     * 
     * @param sub
     *            The sub list
     */
    protected void linkToHead(SortedLinkedList<T> target) {

        if (target.head == null) {
            addToIndex(getThis(), target);
            next = prev = target.head = getThis();
        } else {
            target.head.linkBefore(getThis());
        }
    }

    /**
     * @param node
     *            the node to link after this node.
     * @return this
     */
    protected void linkAfter(T node) {
        checkLinkOk(node);
        addToIndex(node, list);

        // given we linked this<->next and are inserting node in between
        node.prev = getThis(); // link this<-node
        node.next = next; // link node->next
        next.prev = node; // link node<-next
        next = node; // this->node
    }

    /**
     * @param node
     *            the node to link after this node.
     * @return
     * @return this
     */
    protected void linkBefore(T node) {
        checkLinkOk(node);
        addToIndex(node, list);

        // given we linked prev<->this and are inserting node in between
        node.next = getThis(); // node->this
        node.prev = prev; // prev<-node
        prev.next = node; // prev->node
        prev = node; // node<-this

        if (this == list.head) {
            list.head = node;
        }
    }

    public abstract long getSequence(); 

    public boolean isLinked() {
        return list != null;
    }

    public SortedLinkedList<T> getList() {
        return list;
    }
}
