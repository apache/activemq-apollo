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

public interface AmqpSource extends AmqpMap {



    /**
     * the address of the source
     * <p>
     * The address is resolved to a Node in the Container of the sending Link Endpoint.
     * </p>
     * <p>
     * command sent by the sending Link Endpoint. When sent by the receiving Link Endpoint the
     * address MUST be set unless the create flag is set, in which case the address MUST NOT be
     * set.
     * </p>
     */
    public void setAddress(AmqpAddress address);

    /**
     * the address of the source
     * <p>
     * The address is resolved to a Node in the Container of the sending Link Endpoint.
     * </p>
     * <p>
     * command sent by the sending Link Endpoint. When sent by the receiving Link Endpoint the
     * address MUST be set unless the create flag is set, in which case the address MUST NOT be
     * set.
     * </p>
     */
    public AmqpAddress getAddress();

    /**
     * request creation of a remote Node
     * <p>
     * sent by the outgoing Link Endpoint.
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
     * sent by the outgoing Link Endpoint.
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
     * sent by the outgoing Link Endpoint.
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
     * the source timeout
     * <p>
     * The minimum length of time (in milliseconds) after the Session has been destroyed before
     * the source is destroyed. The value set by the receiving Link endpoint is indicative of
     * the timeout it desires, the value set by the sending Link endpoint defines the timeout
     * which will be used.
     * </p>
     */
    public void setTimeout(Long timeout);

    /**
     * the source timeout
     * <p>
     * The minimum length of time (in milliseconds) after the Session has been destroyed before
     * the source is destroyed. The value set by the receiving Link endpoint is indicative of
     * the timeout it desires, the value set by the sending Link endpoint defines the timeout
     * which will be used.
     * </p>
     */
    public void setTimeout(AmqpUint timeout);

    /**
     * the source timeout
     * <p>
     * The minimum length of time (in milliseconds) after the Session has been destroyed before
     * the source is destroyed. The value set by the receiving Link endpoint is indicative of
     * the timeout it desires, the value set by the sending Link endpoint defines the timeout
     * which will be used.
     * </p>
     */
    public Long getTimeout();

    /**
     * the distribution mode of the Link
     * <p>
     * Link.
     * </p>
     */
    public void setDistributionMode(AmqpDistributionMode distributionMode);

    /**
     * the distribution mode of the Link
     * <p>
     * Link.
     * </p>
     */
    public AmqpDistributionMode getDistributionMode();

    /**
     * a set of predicate to filter the Messages admitted onto the Link
     */
    public void setFilter(AmqpFilterSet filter);

    /**
     * a set of predicate to filter the Messages admitted onto the Link
     */
    public AmqpFilterSet getFilter();

    /**
     * states of messages to be considered for sending from the source
     * <p>
     * state.
     * </p>
     */
    public void setMessageStates(IAmqpList messageStates);

    /**
     * states of messages to be considered for sending from the source
     * <p>
     * state.
     * </p>
     */
    public void setMessageStates(AmqpList messageStates);

    /**
     * states of messages to be considered for sending from the source
     * <p>
     * state.
     * </p>
     */
    public IAmqpList getMessageStates();

    /**
     * disposition for unacked Messages
     * <p>
     * value).
     * </p>
     */
    public void setOrphanDisposition(HashMap<AmqpType<?,?>, AmqpType<?,?>> orphanDisposition);

    /**
     * disposition for unacked Messages
     * <p>
     * value).
     * </p>
     */
    public void setOrphanDisposition(AmqpMap orphanDisposition);

    /**
     * disposition for unacked Messages
     * <p>
     * value).
     * </p>
     */
    public HashMap<AmqpType<?,?>, AmqpType<?,?>> getOrphanDisposition();

    public static class AmqpSourceBean implements AmqpSource{

        private AmqpSourceBuffer buffer;
        private AmqpSourceBean bean = this;
        private AmqpAddress address;
        private AmqpBoolean create;
        private AmqpUint timeout;
        private AmqpDistributionMode distributionMode;
        private AmqpFilterSet filter;
        private AmqpList messageStates;
        private AmqpMap orphanDisposition;
        private HashMap<AmqpType<?,?>, AmqpType<?,?>> value;

        public AmqpSourceBean() {
        }

        public AmqpSourceBean(HashMap<AmqpType<?,?>, AmqpType<?,?>> value) {
            this.value = value;
        }

        public AmqpSourceBean(AmqpSource.AmqpSourceBean other) {
            this.bean = other;
        }

        public final AmqpSourceBean copy() {
            return new AmqpSource.AmqpSourceBean(bean);
        }

        public final AmqpSource.AmqpSourceBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpSourceBuffer(marshaller.encode(this));
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

        public final void setDistributionMode(AmqpDistributionMode distributionMode) {
            copyCheck();
            bean.distributionMode = distributionMode;
        }

        public final AmqpDistributionMode getDistributionMode() {
            return bean.distributionMode;
        }

        public final void setFilter(AmqpFilterSet filter) {
            copyCheck();
            bean.filter = filter;
        }

        public final AmqpFilterSet getFilter() {
            return bean.filter;
        }

        public void setMessageStates(IAmqpList messageStates) {
            setMessageStates(new AmqpList.AmqpListBean(messageStates));
        }


        public final void setMessageStates(AmqpList messageStates) {
            copyCheck();
            bean.messageStates = messageStates;
        }

        public final IAmqpList getMessageStates() {
            return bean.messageStates.getValue();
        }

        public void setOrphanDisposition(HashMap<AmqpType<?,?>, AmqpType<?,?>> orphanDisposition) {
            setOrphanDisposition(new AmqpMap.AmqpMapBean(orphanDisposition));
        }


        public final void setOrphanDisposition(AmqpMap orphanDisposition) {
            copyCheck();
            bean.orphanDisposition = orphanDisposition;
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getOrphanDisposition() {
            return bean.orphanDisposition.getValue();
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

        private final void copy(AmqpSource.AmqpSourceBean other) {
            this.address= other.address;
            this.create= other.create;
            this.timeout= other.timeout;
            this.distributionMode= other.distributionMode;
            this.filter= other.filter;
            this.messageStates= other.messageStates;
            this.orphanDisposition= other.orphanDisposition;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpSource)) {
                return false;
            }

            return equivalent((AmqpSource) t);
        }

        public boolean equivalent(AmqpSource b) {

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

            if(b.getDistributionMode() == null ^ getDistributionMode() == null) {
                return false;
            }
            if(b.getDistributionMode() != null && !b.getDistributionMode().equals(getDistributionMode())){ 
                return false;
            }

            if(b.getFilter() == null ^ getFilter() == null) {
                return false;
            }
            if(b.getFilter() != null && !b.getFilter().equals(getFilter())){ 
                return false;
            }

            if(b.getMessageStates() == null ^ getMessageStates() == null) {
                return false;
            }
            if(b.getMessageStates() != null && !b.getMessageStates().equals(getMessageStates())){ 
                return false;
            }

            if(b.getOrphanDisposition() == null ^ getOrphanDisposition() == null) {
                return false;
            }
            if(b.getOrphanDisposition() != null && !b.getOrphanDisposition().equals(getOrphanDisposition())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpSourceBuffer extends AmqpMap.AmqpMapBuffer implements AmqpSource{

        private AmqpSourceBean bean;

        protected AmqpSourceBuffer(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
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

        public final void setDistributionMode(AmqpDistributionMode distributionMode) {
            bean().setDistributionMode(distributionMode);
        }

        public final AmqpDistributionMode getDistributionMode() {
            return bean().getDistributionMode();
        }

        public final void setFilter(AmqpFilterSet filter) {
            bean().setFilter(filter);
        }

        public final AmqpFilterSet getFilter() {
            return bean().getFilter();
        }

    public void setMessageStates(IAmqpList messageStates) {
            bean().setMessageStates(messageStates);
        }

        public final void setMessageStates(AmqpList messageStates) {
            bean().setMessageStates(messageStates);
        }

        public final IAmqpList getMessageStates() {
            return bean().getMessageStates();
        }

    public void setOrphanDisposition(HashMap<AmqpType<?,?>, AmqpType<?,?>> orphanDisposition) {
            bean().setOrphanDisposition(orphanDisposition);
        }

        public final void setOrphanDisposition(AmqpMap orphanDisposition) {
            bean().setOrphanDisposition(orphanDisposition);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getOrphanDisposition() {
            return bean().getOrphanDisposition();
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

        public AmqpSource.AmqpSourceBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpSource bean() {
            if(bean == null) {
                bean = new AmqpSource.AmqpSourceBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpSource.AmqpSourceBuffer create(Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpSource.AmqpSourceBuffer(encoded);
        }

        public static AmqpSource.AmqpSourceBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpSource(in));
        }

        public static AmqpSource.AmqpSourceBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpSource(buffer, offset));
        }
    }
}