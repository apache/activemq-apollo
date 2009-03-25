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
package org.apache.activemq.broker.store.kahadb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.util.Marshaller;

public class MessageKeys {
    public static final MessageKeysMarshaller MARSHALLER = new MessageKeysMarshaller();

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
    
    public static class MessageKeysMarshaller implements Marshaller<MessageKeys> {
        
        public Class<MessageKeys> getType() {
            return MessageKeys.class;
        }

        public MessageKeys readPayload(DataInput dataIn) throws IOException {
            byte data[] = new byte[dataIn.readShort()];
            return new MessageKeys(new AsciiBuffer(data), Marshallers.LOCATION_MARSHALLER.readPayload(dataIn));
        }

        public void writePayload(MessageKeys object, DataOutput dataOut) throws IOException {
            dataOut.writeShort(object.messageId.length);
            dataOut.write(object.messageId.data, object.messageId.offset, object.messageId.length);
            Marshallers.LOCATION_MARSHALLER.writePayload(object.location, dataOut);
        }
    }
}