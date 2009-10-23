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
package org.apache.hawtdb.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides Key/Value storage and retrieval. 
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface Index<Key,Value> extends Iterable<Map.Entry<Key, Value>> {

    /**
     * Frees any extra storage that the index created.
     */
    void destroy();
    
    /**
     * clear the index
     * 
     * @throws IOException
     * 
     */
    void clear();

    /**
     * @param key
     * @return true if it contains the key
     * @throws IOException
     */
    boolean containsKey(Key key);

    /**
     * remove the index key
     * 
     * @param key
     * @return StoreEntry removed
     * @throws IOException
     */
    Value remove(Key key);

    /**
     * store the key, item
     * 
     * @param key
     * @param entry
     * @throws IOException
     */
    Value put(Key key, Value entry);

    /**
     * @param key
     * @return the entry
     * @throws IOException
     */
    Value get(Key key);
    
    int size();
    
    boolean isEmpty();
    
    /**
     * @param tx
     * @return
     * @throws IOException
     * throws UnsupportedOperationException if the index does not support fast iteration of the elements.
     */
    Iterator<Map.Entry<Key,Value>> iterator() throws UnsupportedOperationException;


    /**
     * Traverses the visitor over the stored entries in this index.  The visitor can control
     * which keys and values are visited.
     * 
     * @param visitor
     * @throws UnsupportedOperationException if this is not an ordered index.
     */
    public void visit(IndexVisitor<Key, Value> visitor) throws UnsupportedOperationException;

    int getPage();
    
}
