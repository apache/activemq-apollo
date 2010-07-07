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

import java.util.List;


/**
 * Encodes objects to a page file and decodes them from a page file.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @param <T>
 */
public interface EncoderDecoder<T> {
    
    /**
     * Store a value at the specified page.
     * 
     * Object encoding will be deferred as long as possible.  This allows multiple updates
     * the the same page to avoid paying the cost of multiple encoding passes.  Only when
     * the value is evicted from the cache or the page file is flushed, will the encoding
     * take place.
     * 
     * Since the deferred encoding is no longer taking place in the context of a transaction,
     * there are several restrictions of what pages the Marshaler can update so that transaction
     * isolation is not violated:
     * <ul> 
     * <li>It can update the specified page<li>
     * <li>It can allocate new pages and update or free those pages</li>
     * </ul>
     * 
     * @param paged
     * @param page
     * @param value
     * @return a list of any pages allocated by the method.
     */
    List<Integer> store(Paged paged, int page, T value);
    
    /**
     * Load a value from a specified page.  It should not attempt do any
     * update operations against the {@link Paged} object.
     * 
     * @param paged
     * @param page
     * @return
     */
    T load(Paged paged, int page);
    
    /**
     * Frees any pages associated with the value stored at the given page if any.  Does not free
     * the page supplied.
     * 
     * @param paged
     * @param page
     */
    List<Integer> remove(Paged paged, int page);
}