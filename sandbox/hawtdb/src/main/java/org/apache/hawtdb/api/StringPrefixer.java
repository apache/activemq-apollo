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
 * StringPrefixer is a {@link Prefixer} implementation that works on strings.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class StringPrefixer implements Prefixer<String> {

    /**
     * Example: If value1 is "Hello World" and value 2 is "Help Me" then the
     * result will be: "Help"
     * 
     * @see Prefixer#getSimplePrefix
     */
    public String getSimplePrefix(String value1, String value2) {
        char[] c1 = value1.toCharArray();
        char[] c2 = value2.toCharArray();
        int n = Math.min(c1.length, c2.length);
        int i = 0;
        while (i < n) {
            if (c1[i] != c2[i]) {
                return value2.substring(0, i + 1);
            }
            i++;
        }

        if (n == c2.length) {
            return value2;
        }
        return value2.substring(0, n);
    }
}