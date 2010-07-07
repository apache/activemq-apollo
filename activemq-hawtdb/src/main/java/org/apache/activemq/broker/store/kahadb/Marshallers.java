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

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.fusesource.hawtdb.util.marshaller.Marshaller;
import org.fusesource.hawtdb.util.marshaller.VariableMarshaller;

public class Marshallers {

    public final static Marshaller<QueueRecord> QUEUE_RECORD_MARSHALLER = new VariableMarshaller<QueueRecord>() {

        public QueueRecord readPayload(DataInput dataIn) throws IOException {
            QueueRecord rc = new QueueRecord();
            rc.setQueueKey(dataIn.readLong());
            rc.setMessageKey(dataIn.readLong());
            rc.setSize(dataIn.readInt());
            if (dataIn.readBoolean()) {
                rc.setTte(dataIn.readLong());
            }
            rc.setRedelivered(dataIn.readBoolean());
            if (dataIn.readBoolean()) {
                rc.setAttachment(BUFFER_MARSHALLER.readPayload(dataIn));
            }
            return rc;
        }

        public void writePayload(QueueRecord object, DataOutput dataOut) throws IOException {
            dataOut.writeLong(object.getQueueKey());
            dataOut.writeLong(object.getMessageKey());
            dataOut.writeInt(object.getSize());
            if (object.getTte() >= 0) {
                dataOut.writeBoolean(true);
                dataOut.writeLong(object.getTte());
            } else {
                dataOut.writeBoolean(false);
            }
            dataOut.writeBoolean(object.isRedelivered());
            if (object.getAttachment() != null) {
                dataOut.writeBoolean(true);
                BUFFER_MARSHALLER.writePayload(object.getAttachment(), dataOut);
            } else {
                dataOut.writeBoolean(false);
            }
        }

        public int estimatedSize(QueueRecord object) {
            throw new UnsupportedOperationException();
        }
    };

    public final static Marshaller<QueueDescriptor> QUEUE_DESCRIPTOR_MARSHALLER = new VariableMarshaller<QueueDescriptor>() {

        public QueueDescriptor readPayload(DataInput dataIn) throws IOException {
            QueueDescriptor descriptor = new QueueDescriptor();
            descriptor.setQueueType(dataIn.readShort());
            descriptor.setApplicationType(dataIn.readShort());
            descriptor.setQueueName(ASCII_BUFFER_MARSHALLER.readPayload(dataIn));
            if (dataIn.readBoolean()) {
                descriptor.setParent(ASCII_BUFFER_MARSHALLER.readPayload(dataIn));
                descriptor.setPartitionId(dataIn.readInt());
            }
            return descriptor;
        }

        public void writePayload(QueueDescriptor object, DataOutput dataOut) throws IOException {
            dataOut.writeShort(object.getQueueType());
            dataOut.writeShort(object.getApplicationType());
            ASCII_BUFFER_MARSHALLER.writePayload(object.getQueueName(), dataOut);
            if (object.getParent() != null) {
                dataOut.writeBoolean(true);
                ASCII_BUFFER_MARSHALLER.writePayload(object.getParent(), dataOut);
                dataOut.writeInt(object.getPartitionKey());
            } else {
                dataOut.writeBoolean(false);
            }
        }
        public int estimatedSize(QueueDescriptor object) {
            throw new UnsupportedOperationException();
        }
    };



    static abstract public class AbstractBufferMarshaller<T extends Buffer> extends org.fusesource.hawtdb.util.marshaller.VariableMarshaller<T> {

        public void writePayload(T value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.length);
            dataOut.write(value.data, value.offset, value.length);
        }

        public T readPayload(DataInput dataIn) throws IOException {
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

    public final static Marshaller<AsciiBuffer> ASCII_BUFFER_MARSHALLER = new AbstractBufferMarshaller<AsciiBuffer>() {
        @Override
        protected AsciiBuffer createBuffer(byte[] data) {
            return new AsciiBuffer(data);
        }

    };

    public final static Marshaller<Buffer> BUFFER_MARSHALLER = new AbstractBufferMarshaller<Buffer>() {
        @Override
        protected Buffer createBuffer(byte[] data) {
            return new Buffer(data);
        }
    };

}