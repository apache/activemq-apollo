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
import java.lang.Boolean;
import java.util.HashMap;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a the completed disposition
 * <p>
 * The completed disposition is used to indicate that the Message has been processed at this
 * time, and can be moved to the ARCHIVED state. For a destructive link this is the default
 * presumptive disposition.
 * </p>
 */
public interface AmqpCompleted extends AmqpMap {



    /**
     * permit truncation of the remaining transfer
     * <p>
     * The truncate flag, if true, indicates that the receiver is not interested in the rest of
     * the Message content, and the sender is free to omit it.
     * </p>
     */
    public void setTruncate(Boolean truncate);

    /**
     * permit truncation of the remaining transfer
     * <p>
     * The truncate flag, if true, indicates that the receiver is not interested in the rest of
     * the Message content, and the sender is free to omit it.
     * </p>
     */
    public void setTruncate(AmqpBoolean truncate);

    /**
     * permit truncation of the remaining transfer
     * <p>
     * The truncate flag, if true, indicates that the receiver is not interested in the rest of
     * the Message content, and the sender is free to omit it.
     * </p>
     */
    public Boolean getTruncate();

    public static class AmqpCompletedBean implements AmqpCompleted{

        private AmqpCompletedBuffer buffer;
        private AmqpCompletedBean bean = this;
        private AmqpBoolean truncate;
        private HashMap<AmqpType<?,?>, AmqpType<?,?>> value;

        public AmqpCompletedBean() {
        }

        public AmqpCompletedBean(HashMap<AmqpType<?,?>, AmqpType<?,?>> value) {
            this.value = value;
        }

        public AmqpCompletedBean(AmqpCompleted.AmqpCompletedBean other) {
            this.bean = other;
        }

        public final AmqpCompletedBean copy() {
            return new AmqpCompleted.AmqpCompletedBean(bean);
        }

        public final AmqpCompleted.AmqpCompletedBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpCompletedBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public void setTruncate(Boolean truncate) {
            setTruncate(new AmqpBoolean.AmqpBooleanBean(truncate));
        }


        public final void setTruncate(AmqpBoolean truncate) {
            copyCheck();
            bean.truncate = truncate;
        }

        public final Boolean getTruncate() {
            return bean.truncate.getValue();
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

        private final void copy(AmqpCompleted.AmqpCompletedBean other) {
            this.truncate= other.truncate;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpCompleted)) {
                return false;
            }

            return equivalent((AmqpCompleted) t);
        }

        public boolean equivalent(AmqpCompleted b) {

            if(b.getTruncate() == null ^ getTruncate() == null) {
                return false;
            }
            if(b.getTruncate() != null && !b.getTruncate().equals(getTruncate())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpCompletedBuffer extends AmqpMap.AmqpMapBuffer implements AmqpCompleted{

        private AmqpCompletedBean bean;

        protected AmqpCompletedBuffer(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            super(encoded);
        }

    public void setTruncate(Boolean truncate) {
            bean().setTruncate(truncate);
        }

        public final void setTruncate(AmqpBoolean truncate) {
            bean().setTruncate(truncate);
        }

        public final Boolean getTruncate() {
            return bean().getTruncate();
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

        public AmqpCompleted.AmqpCompletedBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpCompleted bean() {
            if(bean == null) {
                bean = new AmqpCompleted.AmqpCompletedBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpCompleted.AmqpCompletedBuffer create(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpCompleted.AmqpCompletedBuffer(encoded);
        }

        public static AmqpCompleted.AmqpCompletedBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpCompleted(in));
        }

        public static AmqpCompleted.AmqpCompletedBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpCompleted(buffer, offset));
        }
    }
}