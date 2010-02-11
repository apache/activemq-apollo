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
import java.lang.Long;
import java.util.HashMap;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.util.buffer.Buffer;

public interface AmqpTarget extends AmqpMap {



    /**
     * The address of the target.
     * <p>
     * The address is resolved to a Node by the Container of the receiving Link Endpoint.
     * </p>
     * <p>
     * command sent by the receiving Link Endpoint. When sent by the sending Link Endpoint the
     * address MUST be set unless the create flag is set, in which case the address MUST NOT be
     * set.
     * </p>
     */
    public void setAddress(AmqpAddress address);

    /**
     * The address of the target.
     * <p>
     * The address is resolved to a Node by the Container of the receiving Link Endpoint.
     * </p>
     * <p>
     * command sent by the receiving Link Endpoint. When sent by the sending Link Endpoint the
     * address MUST be set unless the create flag is set, in which case the address MUST NOT be
     * set.
     * </p>
     */
    public AmqpAddress getAddress();

    /**
     * request creation of a remote Node
     * <p>
     * sent by the incoming Link Endpoint.
     * </p>
     * <p>
     * The algorithm used to produce the address from the Link name, must produce repeatable
     * results. If the Link is durable, generating an address from a given Link name within a
     * given client-id MUST always produce the same result. If the Link is not durable,
     * generating an address from a given Link name within a given Session MUST always produce
     * the same result. The generated address SHOULD include the Link name and Session-name or
     * client-id in some recognizable form for ease of traceability.
     * </p>
     */
    public void setCreate(Boolean create);

    /**
     * request creation of a remote Node
     * <p>
     * sent by the incoming Link Endpoint.
     * </p>
     * <p>
     * The algorithm used to produce the address from the Link name, must produce repeatable
     * results. If the Link is durable, generating an address from a given Link name within a
     * given client-id MUST always produce the same result. If the Link is not durable,
     * generating an address from a given Link name within a given Session MUST always produce
     * the same result. The generated address SHOULD include the Link name and Session-name or
     * client-id in some recognizable form for ease of traceability.
     * </p>
     */
    public void setCreate(AmqpBoolean create);

    /**
     * request creation of a remote Node
     * <p>
     * sent by the incoming Link Endpoint.
     * </p>
     * <p>
     * The algorithm used to produce the address from the Link name, must produce repeatable
     * results. If the Link is durable, generating an address from a given Link name within a
     * given client-id MUST always produce the same result. If the Link is not durable,
     * generating an address from a given Link name within a given Session MUST always produce
     * the same result. The generated address SHOULD include the Link name and Session-name or
     * client-id in some recognizable form for ease of traceability.
     * </p>
     */
    public Boolean getCreate();

    /**
     * the target timeout
     * <p>
     * The minimum length of time (in milliseconds) after the Link has been destroyed before
     * the target is destroyed. The value set by the sending Link endpoint is indicative of
     * the timeout it desires, the value set by the receiving Link endpoint defines the timeout
     * which will be used.
     * </p>
     */
    public void setTimeout(Long timeout);

    /**
     * the target timeout
     * <p>
     * The minimum length of time (in milliseconds) after the Link has been destroyed before
     * the target is destroyed. The value set by the sending Link endpoint is indicative of
     * the timeout it desires, the value set by the receiving Link endpoint defines the timeout
     * which will be used.
     * </p>
     */
    public void setTimeout(AmqpUint timeout);

    /**
     * the target timeout
     * <p>
     * The minimum length of time (in milliseconds) after the Link has been destroyed before
     * the target is destroyed. The value set by the sending Link endpoint is indicative of
     * the timeout it desires, the value set by the receiving Link endpoint defines the timeout
     * which will be used.
     * </p>
     */
    public Long getTimeout();

    public static class AmqpTargetBean implements AmqpTarget{

        private AmqpTargetBuffer buffer;
        private AmqpTargetBean bean = this;
        private AmqpAddress address;
        private AmqpBoolean create;
        private AmqpUint timeout;
        private HashMap<AmqpType<?,?>, AmqpType<?,?>> value;

        public AmqpTargetBean() {
        }

        public AmqpTargetBean(HashMap<AmqpType<?,?>, AmqpType<?,?>> value) {
            this.value = value;
        }

        public AmqpTargetBean(AmqpTarget.AmqpTargetBean other) {
            this.bean = other;
        }

        public final AmqpTargetBean copy() {
            return new AmqpTarget.AmqpTargetBean(bean);
        }

        public final AmqpTarget.AmqpTargetBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpTargetBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public final void setAddress(AmqpAddress address) {
            copyCheck();
            bean.address = address;
        }

        public final AmqpAddress getAddress() {
            return bean.address;
        }

        public void setCreate(Boolean create) {
            setCreate(new AmqpBoolean.AmqpBooleanBean(create));
        }


        public final void setCreate(AmqpBoolean create) {
            copyCheck();
            bean.create = create;
        }

        public final Boolean getCreate() {
            return bean.create.getValue();
        }

        public void setTimeout(Long timeout) {
            setTimeout(new AmqpUint.AmqpUintBean(timeout));
        }


        public final void setTimeout(AmqpUint timeout) {
            copyCheck();
            bean.timeout = timeout;
        }

        public final Long getTimeout() {
            return bean.timeout.getValue();
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

        private final void copy(AmqpTarget.AmqpTargetBean other) {
            this.address= other.address;
            this.create= other.create;
            this.timeout= other.timeout;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpTarget)) {
                return false;
            }

            return equivalent((AmqpTarget) t);
        }

        public boolean equivalent(AmqpTarget b) {

            if(b.getAddress() == null ^ getAddress() == null) {
                return false;
            }
            if(b.getAddress() != null && !b.getAddress().equals(getAddress())){ 
                return false;
            }

            if(b.getCreate() == null ^ getCreate() == null) {
                return false;
            }
            if(b.getCreate() != null && !b.getCreate().equals(getCreate())){ 
                return false;
            }

            if(b.getTimeout() == null ^ getTimeout() == null) {
                return false;
            }
            if(b.getTimeout() != null && !b.getTimeout().equals(getTimeout())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpTargetBuffer extends AmqpMap.AmqpMapBuffer implements AmqpTarget{

        private AmqpTargetBean bean;

        protected AmqpTargetBuffer(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            super(encoded);
        }

        public final void setAddress(AmqpAddress address) {
            bean().setAddress(address);
        }

        public final AmqpAddress getAddress() {
            return bean().getAddress();
        }

    public void setCreate(Boolean create) {
            bean().setCreate(create);
        }

        public final void setCreate(AmqpBoolean create) {
            bean().setCreate(create);
        }

        public final Boolean getCreate() {
            return bean().getCreate();
        }

    public void setTimeout(Long timeout) {
            bean().setTimeout(timeout);
        }

        public final void setTimeout(AmqpUint timeout) {
            bean().setTimeout(timeout);
        }

        public final Long getTimeout() {
            return bean().getTimeout();
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

        public AmqpTarget.AmqpTargetBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpTarget bean() {
            if(bean == null) {
                bean = new AmqpTarget.AmqpTargetBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpTarget.AmqpTargetBuffer create(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpTarget.AmqpTargetBuffer(encoded);
        }

        public static AmqpTarget.AmqpTargetBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpTarget(in));
        }

        public static AmqpTarget.AmqpTargetBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpTarget(buffer, offset));
        }
    }
}