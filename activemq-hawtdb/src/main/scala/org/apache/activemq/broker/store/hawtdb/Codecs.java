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
package org.apache.activemq.broker.store.hawtdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.apollo.store.QueueRecord;
import org.apache.activemq.apollo.store.QueueEntryRecord;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.codec.AsciiBufferCodec;
import org.fusesource.hawtbuf.codec.BufferCodec;
import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtbuf.codec.VariableCodec;

public class Codecs {

    public final static Codec<QueueEntryRecord> QUEUE_RECORD_CODEC = new VariableCodec<QueueEntryRecord>() {

        public QueueEntryRecord decode(DataInput dataIn) throws IOException {
            QueueEntryRecord rc = new QueueEntryRecord();
            rc.queueKey = dataIn.readLong();
            rc.messageKey = dataIn.readLong();
            rc.size = dataIn.readInt();
//            if (dataIn.readBoolean()) {
//                rc.setTte(dataIn.readLong());
//            }
            rc.redeliveries = dataIn.readShort();
            if (dataIn.readBoolean()) {
                rc.attachment = BUFFER_CODEC.decode(dataIn);
            }
            return rc;
        }

        public void encode(QueueEntryRecord object, DataOutput dataOut) throws IOException {
            dataOut.writeLong(object.queueKey);
            dataOut.writeLong(object.messageKey);
            dataOut.writeInt(object.size);
//            if (object.getTte() >= 0) {
//                dataOut.writeBoolean(true);
//                dataOut.writeLong(object.getTte());
//            } else {
//                dataOut.writeBoolean(false);
//            }
            dataOut.writeShort(object.redeliveries);
            if (object.attachment != null) {
                dataOut.writeBoolean(true);
                BUFFER_CODEC.encode(object.attachment, dataOut);
            } else {
                dataOut.writeBoolean(false);
            }
        }

        public int estimatedSize(QueueEntryRecord object) {
            throw new UnsupportedOperationException();
        }
    };

    public final static Codec<QueueRecord> QUEUE_DESCRIPTOR_CODEC = new VariableCodec<QueueRecord>() {

        public QueueRecord decode(DataInput dataIn) throws IOException {
            QueueRecord record = new QueueRecord();
            record.queueType = ASCII_BUFFER_CODEC.decode(dataIn);
            record.name = ASCII_BUFFER_CODEC.decode(dataIn);
//            if (dataIn.readBoolean()) {
//                record.parent = ASCII_BUFFER_MARSHALLER.readPayload(dataIn)
//                record.setPartitionId(dataIn.readInt());
//            }
            return record;
        }

        public void encode(QueueRecord object, DataOutput dataOut) throws IOException {
            ASCII_BUFFER_CODEC.encode(object.queueType, dataOut);
            ASCII_BUFFER_CODEC.encode(object.name, dataOut);
//            if (object.parent != null) {
//                dataOut.writeBoolean(true);
//                ASCII_BUFFER_MARSHALLER.writePayload(object.parent, dataOut);
//                dataOut.writeInt(object.getPartitionKey());
//            } else {
//                dataOut.writeBoolean(false);
//            }
        }
        public int estimatedSize(QueueRecord object) {
            throw new UnsupportedOperationException();
        }
    };



    static abstract public class AbstractBufferCodec<T extends Buffer> extends VariableCodec<T> {

        public void encode(T value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.length);
            dataOut.write(value.data, value.offset, value.length);
        }

        public T decode(DataInput dataIn) throws IOException {
            int size = dataIn.readInt();
            byte[] data = new byte[size];
            dataIn.readFully(data);
            return createBuffer(data);
        }

        abstract protected T createBuffer(byte [] data);

        public T deepCopy(T source) {
            return createBuffer(source.deepCopy().data);
        }

        public boolean isDeepCopySupported() {
            return true;
        }

        public int estimatedSize(T object) {
            return object.length+4;
        }

    }

    public final static AsciiBufferCodec ASCII_BUFFER_CODEC = AsciiBufferCodec.INSTANCE;
    public final static BufferCodec BUFFER_CODEC = BufferCodec.INSTANCE;

}