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
package org.apache.activemq.queue;

import java.util.Iterator;

public interface Store<K, V> {

    public interface StoreNode<K, V> {

        public boolean acquire(Subscription<V> ownerId);

        public void unacquire();

        public V getValue();

        public K getKey();

    }

    public interface StoreCursor<K, V> extends Iterator<StoreNode<K, V>> {
        public StoreNode<K, V> peekNext();

        public void setNext(StoreNode<K, V> node);

    }

    StoreNode<K, V> remove(K key);

    StoreNode<K, V> add(K key, V value);

    StoreCursor<K, V> openCursor();

    StoreCursor<K, V> openCursorAt(StoreNode<K, V> next);

    boolean isEmpty();

    int size();

}
