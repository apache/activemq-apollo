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

import org.fusesource.hawtbuf.Buffer;

public interface WireFormatFactory {
    
    /**
     * @return an instance of the wire format. 
     * 
     */
    WireFormat createWireFormat();
    
    /**
     * @return true if this wire format factory is isDiscriminatable. A discriminatable
     * WireFormat's will first write a header to the stream 
     */
    boolean isDiscriminatable();
    
    /**
     * @return Returns the maximum length of the header used to discriminate the wire format if it
     * {@link #isDiscriminatable()}
     * @throws UnsupportedOperationException If {@link #isDiscriminatable()} is false
     */
    int maxWireformatHeaderLength();

    /**
     * Called to test if this wireformat matches the provided header.
     * 
     * @param buffer The byte buffer representing the header data read so far.
     * @return true if the Buffer matches the wire format header.
     */
    boolean matchesWireformatHeader(Buffer buffer);

    
}
