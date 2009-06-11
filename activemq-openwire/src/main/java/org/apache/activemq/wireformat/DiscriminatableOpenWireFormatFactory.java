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
package org.apache.activemq.wireformat;

import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.util.ByteSequence;

public class DiscriminatableOpenWireFormatFactory extends OpenWireFormatFactory {

    private static final byte MAGIC[] = new byte[] { 1, 'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q' };

    public boolean isDiscriminatable() {
        return true;
    }

    public boolean matchesWireformatHeader(ByteSequence byteSequence) {
        if (byteSequence.length == 4 + MAGIC.length) {
            for (int i = 0; i < MAGIC.length; i++) {
                if (byteSequence.data[i + 4] != MAGIC[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int maxWireformatHeaderLength() {
        return 4 + MAGIC.length;
    }
}
