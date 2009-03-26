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

import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.util.Marshaller;

public class Marshallers {
    
    public final static Marshaller<QueueRecord> QUEUE_RECORD_MARSHALLER = new Marshaller<QueueRecord>() {
        
        public Class<QueueRecord> getType() {
            return QueueRecord.class;
        }
    
        public QueueRecord readPayload(DataInput dataIn) throws IOException {
            QueueRecord rc = new QueueRecord();
            rc.setQueueKey(dataIn.readLong());
            rc.setMessageKey(dataIn.readLong());
            if( dataIn.readBoolean() ) {
                rc.setAttachment(BUFFER_MARSHALLER.readPayload(dataIn));
            }
            return rc;
        }
    
        public void writePayload(QueueRecord object, DataOutput dataOut) throws IOException {
            dataOut.writeLong(object.getQueueKey());
            dataOut.writeLong(object.getMessageKey());
            if( object.getAttachment()!=null ) {
                dataOut.writeBoolean(true);
                BUFFER_MARSHALLER.writePayload(object.getAttachment(), dataOut);
            } else {
                dataOut.writeBoolean(false);
            }
        }
    };

    public final static Marshaller<Location> LOCATION_MARSHALLER = new Marshaller<Location>() {
    
        public Class<Location> getType() {
            return Location.class;
        }
    
        public Location readPayload(DataInput dataIn) throws IOException {
            Location rc = new Location();
            rc.setDataFileId(dataIn.readInt());
            rc.setOffset(dataIn.readInt());
            return rc;
        }
    
        public void writePayload(Location object, DataOutput dataOut) throws IOException {
            dataOut.writeInt(object.getDataFileId());
            dataOut.writeInt(object.getOffset());
        }
    };
    
    
    public final static Marshaller<AsciiBuffer> ASCII_BUFFER_MARSHALLER = new Marshaller<AsciiBuffer>() {
    
        public Class<AsciiBuffer> getType() {
            return AsciiBuffer.class;
        }
    
        public AsciiBuffer readPayload(DataInput dataIn) throws IOException {
            byte data[] = new byte[dataIn.readShort()];
            dataIn.readFully(data);
            return new AsciiBuffer(data);
        }
    
        public void writePayload(AsciiBuffer object, DataOutput dataOut) throws IOException {
            dataOut.writeShort(object.length);
            dataOut.write(object.data, object.offset, object.length);
        }
    };
    
    public final static Marshaller<Buffer> BUFFER_MARSHALLER = new Marshaller<Buffer>() {
    
        public Class<Buffer> getType() {
            return Buffer.class;
        }
    
        public Buffer readPayload(DataInput dataIn) throws IOException {
            byte data[] = new byte[dataIn.readShort()];
            dataIn.readFully(data);
            return new Buffer(data);
        }
    
        public void writePayload(Buffer object, DataOutput dataOut) throws IOException {
            dataOut.writeShort(object.length);
            dataOut.write(object.data, object.offset, object.length);
        }
    };
}