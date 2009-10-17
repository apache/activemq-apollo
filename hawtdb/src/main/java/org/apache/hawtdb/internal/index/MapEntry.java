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

import java.util.Map;

/**
 * A basic implementation of {@link Map.Entry}.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class MapEntry<Key, Value> implements Map.Entry<Key, Value> {
    
    private final Key key;
    private final Value value;

    public MapEntry(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public Key getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    public Value setValue(Value value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "{ key: "+key+", value: "+value+" }";
    }
}