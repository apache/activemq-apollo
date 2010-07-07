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

/**
 * Interface used to determine the simple prefix of two keys.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface Prefixer<Key> {

    /**
     * This methods should return shortest prefix of value2 where the
     * following still holds:<br/>
     * value1 <= prefix <= value2.<br/>
     * <br/>
     * 
     * When this method is called, the following is guaranteed:<br/>
     * value1 < value2<br/>
     * <br/>
     * 
     * 
     * @param value1
     * @param value2
     * @return
     */
    public Key getSimplePrefix(Key value1, Key value2);
}