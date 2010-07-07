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
package org.apache.activemq.apollo.stomp;

import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

/**
 * Creates WireFormat objects that marshalls the <a href="http://activemq.apache.org/stomp/">Stomp</a> protocol.
 */
public class StompWireFormatFactory implements WireFormatFactory {
    AsciiBuffer MAGIC = new AsciiBuffer("CONNECT");
    
    public WireFormat createWireFormat() {
        return new StompWireFormat();
    }
    
    public boolean isDiscriminatable() {
        return true;
    }

    public int maxWireformatHeaderLength() {
        return MAGIC.length+10;
    }

    public boolean matchesWireformatHeader(Buffer header) {
        if( header.length < MAGIC.length)
            return false;
        
        // the magic can be preceded with newlines..
        int max = header.length-MAGIC.length;
        int start=0;
        while(start < max) {
            if( header.get(start)!='\n' ) {
                break;
            }
            start++;
        }
        
        return header.containsAt(MAGIC, start);
    }


}
