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

import java.util.Arrays;

public class PriorityMap<E> {

    int first;
    int base;
    int size;

    Object elements[] = new Object[1];

    public E put(int key, E value) {
        E rc = null;
        if (isEmpty()) {
            // This will be the first base prioritu..
            base = key;
            elements[0] = value;
            first = 0;
        } else {
            if (key > base) {
                // New priority is after the current base, we may need to
                // expaned the
                // priority array to fit this new one in.
                int index = key - base;
                if (elements.length <= index) {
                    // The funky thing is if the original base was removed,
                    // resizing
                    // will rebase the at the first.
                    resize(index + 1, 0);
                }
                if (index < first) {
                    first = index;
                }
                rc = element(index);
                elements[index] = value;
            } else {
                // Ok this element is before the current base so we need to
                // resize/rebase
                // using this element as the base.
                int oldLastIndex = indexOfLast();
                int newLastIndex = (base + oldLastIndex) - key;
                resize(newLastIndex + 1, first + (base - key), (oldLastIndex - first) + 1);
                elements[0] = value;
                first = 0;
            }
        }
        if (rc == null) {
            size++;
        }
        return rc;
    }

    private int indexOfLast() {
        int i = elements.length - 1;
        while (i >= 0) {
            if (elements[i] != null) {
                return i;
            }
            i--;
        }
        return -1;
    }

    private void resize(int newSize, int firstOffset) {
        int count = Math.min(elements.length - first, newSize);
        resize(newSize, firstOffset, count);
    }

    private void resize(int newSize, int firstOffset, int copyCount) {
        Object t[];
        if (elements.length == newSize) {
            t = elements;
            System.arraycopy(elements, first, t, firstOffset, copyCount);
            Arrays.fill(t, 0, firstOffset, null);
        } else {
            t = new Object[newSize];
            System.arraycopy(elements, first, t, firstOffset, copyCount);
        }
        base += (first - firstOffset);
        elements = t;
    }

    public E get(int priority) {
        int index = priority - base;
        if (index < 0 || index >= elements.length) {
            return null;
        }
        return element(index);
    }

    @SuppressWarnings("unchecked")
    private E element(int index) {
        return (E) elements[index];
    }

    public E remove(int priority) {
        int index = priority - base;
        if (index < 0 || index >= elements.length) {
            return null;
        }
        E rc = element(index);
        elements[index] = null;
        if (rc != null) {
            size--;
        }
        return rc;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public E firstValue() {
        if (size == 0) {
            return null;
        }
        E rc = element(first);
        while (rc == null) {
            // The first element may have been removed so we need to find it...
            first++;
            rc = element(first);
        }
        return (E) rc;
    }

    public Integer firstKey() {
        if (size == 0) {
            return null;
        }
        E rc = element(first);
        while (rc == null) {
            // The first element may have been removed so we need to find it...
            first++;
            rc = element(first);
        }
        return first;
    }

}