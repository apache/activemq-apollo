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
import java.util.HashMap;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a a polymorphic mapping from distinct keys to values
 */
public interface AmqpMap extends AmqpType<AmqpMap.AmqpMapBean, AmqpMap.AmqpMapBuffer> {

    public void put(AmqpType<?, ?> key, AmqpType<?, ?> value);
    public AmqpType<?, ?> get(AmqpType<?, ?> key);

    public HashMap<AmqpType<?,?>, AmqpType<?,?>> getValue();

    public static class AmqpMapBean implements AmqpMap{

        private AmqpMapBuffer buffer;
        private AmqpMapBean bean = this;
        private HashMap<AmqpType<?,?>, AmqpType<?,?>> value;

        protected AmqpMapBean() {
        }

        public AmqpMapBean(HashMap<AmqpType<?,?>, AmqpType<?,?>> value) {
            this.value = value;
        }

        public AmqpMapBean(AmqpMap.AmqpMapBean other) {
            this.bean = other;
        }

        public final AmqpMapBean copy() {
            return new AmqpMap.AmqpMapBean(bean);
        }

        public final AmqpMap.AmqpMapBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpMapBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }

        public void put(AmqpType<?, ?> key, AmqpType<?, ?> value) {
            bean.value.put(key, value);
        }

        public AmqpType<?, ?> get(AmqpType<?, ?> key) {
            return bean.value.get(key);
        }

        public HashMap<AmqpType<?,?>, AmqpType<?,?>> getValue() {
            return bean.value;
        }


        private final void copyCheck() {
            if(buffer != null) {;
                throw new IllegalStateException("unwriteable");
            }
            if(bean != this) {;
                copy(bean);
            }
        }

        private final void copy(AmqpMap.AmqpMapBean other) {
            this.value = other.value;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpMap)) {
                return false;
            }

            return equivalent((AmqpMap) t);
        }

        public boolean equivalent(AmqpMap b) {
            if(b == null) {
                return false;
            }

            if(b.getValue() == null ^ getValue() == null) {
                return false;
            }

            return b.getValue() == null || b.getValue().equals(getValue());
        }
    }

    public static class AmqpMapBuffer implements AmqpMap, AmqpBuffer< HashMap<AmqpType<?,?>, AmqpType<?,?>>> {

        private AmqpMapBean bean;
        protected Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded;

        protected AmqpMapBuffer() {
        }

        protected AmqpMapBuffer(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            this.encoded = encoded;
        }

        public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> getEncoded() throws AmqpEncodingError{
            return encoded;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            encoded.marshal(out);
        }
        public void put(AmqpType<?, ?> key, AmqpType<?, ?> value) {
            bean().put(key, value);
        }

        public AmqpType<?, ?> get(AmqpType<?, ?> key) {
            return bean().get(key);
        }

        public HashMap<AmqpType<?,?>, AmqpType<?,?>> getValue() {
            return bean().getValue();
        }

        public AmqpMap.AmqpMapBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpMap bean() {
            if(bean == null) {
                bean = new AmqpMap.AmqpMapBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpMap.AmqpMapBuffer create(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpMap.AmqpMapBuffer(encoded);
        }

        public static AmqpMap.AmqpMapBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpMap(in));
        }

        public static AmqpMap.AmqpMapBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpMap(buffer, offset));
        }
    }
}