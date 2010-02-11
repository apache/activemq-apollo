/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * his work for additional information regarding copyright ownership.
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
package org.apache.activemq.amqp.protocol.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a a sequence of octets
 */
public interface AmqpBinary extends AmqpType<AmqpBinary.AmqpBinaryBean, AmqpBinary.AmqpBinaryBuffer> {


    public Buffer getValue();

    public static class AmqpBinaryBean implements AmqpBinary{

        private AmqpBinaryBuffer buffer;
        private AmqpBinaryBean bean = this;
        private Buffer value;

        protected AmqpBinaryBean() {
        }

        public AmqpBinaryBean(Buffer value) {
            this.value = value;
        }

        public AmqpBinaryBean(AmqpBinary.AmqpBinaryBean other) {
            this.bean = other;
        }

        public final AmqpBinaryBean copy() {
            return bean;
        }

        public final AmqpBinary.AmqpBinaryBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpBinaryBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public Buffer getValue() {
            return bean.value;
        }


        public boolean equals(Object o){
            if(this == o) {
                return true;
            }

            if(o == null || !(o instanceof AmqpBinary)) {
                return false;
            }

            return equivalent((AmqpBinary) o);
        }

        public int hashCode() {
            if(getValue() == null) {
                return AmqpBinary.AmqpBinaryBean.class.hashCode();
            }
            return getValue().hashCode();
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpBinary)) {
                return false;
            }

            return equivalent((AmqpBinary) t);
        }

        public boolean equivalent(AmqpBinary b) {
            if(b == null) {
                return false;
            }

            if(b.getValue() == null ^ getValue() == null) {
                return false;
            }

            return b.getValue() == null || b.getValue().equals(getValue());
        }
    }

    public static class AmqpBinaryBuffer implements AmqpBinary, AmqpBuffer< Buffer> {

        private AmqpBinaryBean bean;
        protected Encoded<Buffer> encoded;

        protected AmqpBinaryBuffer() {
        }

        protected AmqpBinaryBuffer(Encoded<Buffer> encoded) {
            this.encoded = encoded;
        }

        public final Encoded<Buffer> getEncoded() throws AmqpEncodingError{
            return encoded;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            encoded.marshal(out);
        }

        public Buffer getValue() {
            return bean().getValue();
        }

        public AmqpBinary.AmqpBinaryBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpBinary bean() {
            if(bean == null) {
                bean = new AmqpBinary.AmqpBinaryBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equals(Object o){
            return bean().equals(o);
        }

        public int hashCode() {
            return bean().hashCode();
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpBinary.AmqpBinaryBuffer create(Encoded<Buffer> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpBinary.AmqpBinaryBuffer(encoded);
        }

        public static AmqpBinary.AmqpBinaryBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpBinary(in));
        }

        public static AmqpBinary.AmqpBinaryBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpBinary(buffer, offset));
        }
    }
}