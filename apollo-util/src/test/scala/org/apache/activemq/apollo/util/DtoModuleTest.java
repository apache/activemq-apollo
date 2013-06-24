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
package org.apache.activemq.apollo.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class DtoModuleTest {

    @Test
    public void testDtoSingletons() {
        assertEquals(3, DtoModule$.MODULE$.extension_classes().length);

        // why should this return 3?? there is really only one pacakge
        assertEquals(3, DtoModule$.MODULE$.packages().length);
    }
}
