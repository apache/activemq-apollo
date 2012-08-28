/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.boot;

import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class ApolloTest {
    private static final String DIR_SEPARATOR = ";";

    @Test
    public void testResolveBootDirsAsSeenOnWindows() {
        LinkedList<String> args = new LinkedList<String>();
        args.add("D:\\apache-apollo-1.4\\lib"+ DIR_SEPARATOR +"D:\\apache-apollo\\myboker\\lib");
        String[] bootDirs = Apollo.resolveBootDirs(args);
        assertEquals("The correct number of boot dirs was not found", 2, bootDirs.length);
    }

    @Test
    public void testResolveBootDirsAsSeenOnLinux() {
        LinkedList<String> args = new LinkedList<String>();
        args.add("/Users/cposta/dev/apache-apollo-1.4/lib"+ DIR_SEPARATOR+"/Users/cposta/dev/apache-apollo-1.4/mybroker/lib");
        String[] bootDirs = Apollo.resolveBootDirs(args);
        assertEquals("The correct number of boot dirs was not found", 2, bootDirs.length);
    }
}
