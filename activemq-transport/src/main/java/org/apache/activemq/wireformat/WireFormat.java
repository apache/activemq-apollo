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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.fusesource.hawtbuf.Buffer;


/**
 * Provides a mechanism to marshal commands into and out of packets
 * or into and out of streams, Channels and Datagrams.
 *
 * @version $Revision: 1.1 $
 */
public interface WireFormat {

    enum BufferState {
        EMPTY,
        WAS_EMPTY,
        NOT_EMPTY,
        FULL,
    }

    /**
     * @return The name of the wireformat
     */
    String getName();

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
     * Stream based un-marshaling
     */
    Object unmarshal(DataInput in) throws IOException;

    /**
     * @param channel
     */
    public void setReadableByteChannel(ReadableByteChannel channel);

    /**
     * Non-blocking channel based decoding.
     * 
     * @return
     * @throws IOException
     */
    Object read() throws IOException;

    /**
     * Pushes back a buffer as being unread.  The protocol
     * discriminator may do this before before any reads occur.
     *
     * @param buffer
     */
    void unread(Buffer buffer);


    /**
     * @return The number of bytes received.
     */
    public long getReadCounter();


    public void setWritableByteChannel(WritableByteChannel channel);

    /**
     * Non-blocking channel based encoding.
     *
     * @return true if the write completed.
     * @throws IOException
     */
    BufferState write(Object value) throws IOException;

    /**
     * Attempts to complete the previous write which did not complete.
     * @return
     * @throws IOException
     */
    BufferState flush() throws IOException;

    /**
     * @return The number of bytes written.
     */
    public long getWriteCounter() ;


//    void unmarshalStartPos(int pos);
//
//    int unmarshalEndPos();
//    void unmarshalEndPos(int pos);
//
//    /**
//     * For a unmarshal session is used for non-blocking
//     * unmarshalling.
//     */
//    Object unmarshalNB(ByteBuffer buffer) throws IOException;


}
