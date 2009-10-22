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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class BTreeIterator<Key, Value> implements Iterator<Map.Entry<Key, Value>> {

    private final BTreeIndex<Key, Value> index;
    BTreeNode<Key, Value> current;
    int nextIndex;
    Map.Entry<Key, Value> nextEntry;

    BTreeIterator(BTreeIndex<Key, Value> index, BTreeNode<Key, Value> current, int nextIndex) {
        this.index = index;
        this.current = current;
        this.nextIndex = nextIndex;
    }

    private void findNextPage() {
        if (nextEntry != null) {
            return;
        }

        while (current != null) {
            if (nextIndex >= current.data.keys.length) {
                // we need to roll to the next leaf..
                if (current.data.next >= 0) {
                    current = index.loadNode(null, current.data.next);
                    nextIndex = 0;
                } else {
                    break;
                }
            } else {
                nextEntry = new MapEntry<Key, Value>(current.data.keys[nextIndex], current.data.values[nextIndex]);
                nextIndex++;
                break;
            }

        }
    }

    public boolean hasNext() {
        findNextPage();
        return nextEntry != null;
    }

    public Entry<Key, Value> next() {
        findNextPage();
        if (nextEntry != null) {
            Entry<Key, Value> lastEntry = nextEntry;
            nextEntry = null;
            return lastEntry;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}