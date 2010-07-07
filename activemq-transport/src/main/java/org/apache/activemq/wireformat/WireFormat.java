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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.buffer.Buffer;


/**
 * Provides a mechanism to marshal commands into and out of packets
 * or into and out of streams, Channels and Datagrams.
 *
 * @version $Revision: 1.1 $
 */
public interface WireFormat {

    /**
     * Packet based marshaling 
     */
    Buffer marshal(Object command) throws IOException;
    
    /**
     * Packet based un-marshaling 
     */
    Object unmarshal(Buffer packet) throws IOException;

    /**
     * Stream based marshaling 
     */
    void marshal(Object command, DataOutput out) throws IOException;
    
    /**
     * Packet based un-marshaling 
     */
    Object unmarshal(DataInput in) throws IOException;

    Object unmarshal(ReadableByteChannel channel);

    /**
     * @return The name of the wireformat
     */
    String getName();
    
    /**
     * Returns a WireFormatFactory which can create WireFormat of this type.
     * @return
     */
    WireFormatFactory getWireFormatFactory();

}
