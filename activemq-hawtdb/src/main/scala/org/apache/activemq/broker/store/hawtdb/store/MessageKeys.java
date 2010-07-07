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
package org.apache.activemq.broker.store.hawtdb.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.apache.activemq.util.marshaller.Marshaller;
import org.apache.activemq.util.marshaller.VariableMarshaller;
import org.fusesource.hawtdb.internal.journal.Location;
import org.fusesource.hawtdb.util.marshaller.LocationMarshaller;

public class MessageKeys {

    final AsciiBuffer messageId;
    final Location location;
    
    public MessageKeys(AsciiBuffer messageId, Location location) {
        this.messageId=messageId;
        this.location=location;
    }
    
    @Override
    public String toString() {
        return "["+messageId+","+location+"]";
    }
    
    public static final Marshaller<MessageKeys> MARSHALLER = new VariableMarshaller<MessageKeys>() {
        public MessageKeys readPayload(DataInput dataIn) throws IOException {
            Location location = LocationMarshaller.INSTANCE.readPayload(dataIn);
            byte data[] = new byte[dataIn.readShort()];
            dataIn.readFully(data);
            return new MessageKeys(new AsciiBuffer(data), location);
        }

        public void writePayload(MessageKeys object, DataOutput dataOut) throws IOException {
            LocationMarshaller.INSTANCE.writePayload(object.location, dataOut);
            dataOut.writeShort(object.messageId.length);
            dataOut.write(object.messageId.data, object.messageId.offset, object.messageId.length);
        }

        public int estimatedSize(MessageKeys object) {
            throw new UnsupportedOperationException();
        }
    };
}