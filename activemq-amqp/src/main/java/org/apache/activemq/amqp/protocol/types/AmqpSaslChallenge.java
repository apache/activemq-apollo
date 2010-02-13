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
import java.util.Iterator;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a security mechanism challenge
 * <p>
 * Send the SASL challenge data as defined by the SASL specification.
 * </p>
 */
public interface AmqpSaslChallenge extends AmqpList {



    /**
     * options map
     */
    public void setOptions(AmqpMap options);

    /**
     * options map
     */
    public IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>> getOptions();

    /**
     * security challenge data
     * <p>
     * Challenge information, a block of opaque binary data passed to the security
     * mechanism.
     * </p>
     */
    public void setChallenge(Buffer challenge);

    /**
     * security challenge data
     * <p>
     * Challenge information, a block of opaque binary data passed to the security
     * mechanism.
     * </p>
     */
    public void setChallenge(AmqpBinary challenge);

    /**
     * security challenge data
     * <p>
     * Challenge information, a block of opaque binary data passed to the security
     * mechanism.
     * </p>
     */
    public Buffer getChallenge();

    public static class AmqpSaslChallengeBean implements AmqpSaslChallenge{

        private AmqpSaslChallengeBuffer buffer;
        private AmqpSaslChallengeBean bean = this;
        private AmqpMap options;
        private AmqpBinary challenge;

        AmqpSaslChallengeBean() {
        }

        AmqpSaslChallengeBean(IAmqpList<AmqpType<?, ?>> value) {

            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        AmqpSaslChallengeBean(AmqpSaslChallenge.AmqpSaslChallengeBean other) {
            this.bean = other;
        }

        public final AmqpSaslChallengeBean copy() {
            return new AmqpSaslChallenge.AmqpSaslChallengeBean(bean);
        }

        public final AmqpSaslChallenge.AmqpSaslChallengeBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpSaslChallengeBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public final void setOptions(AmqpMap options) {
            copyCheck();
            bean.options = options;
        }

        public final IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>> getOptions() {
            return bean.options;
        }

        public void setChallenge(Buffer challenge) {
            setChallenge(TypeFactory.createAmqpBinary(challenge));
        }


        public final void setChallenge(AmqpBinary challenge) {
            copyCheck();
            bean.challenge = challenge;
        }

        public final Buffer getChallenge() {
            return bean.challenge.getValue();
        }

        public void set(int index, AmqpType<?, ?> value) {
            switch(index) {
            case 0: {
                setOptions((AmqpMap) value);
                break;
            }
            case 1: {
                setChallenge((AmqpBinary) value);
                break;
            }
            default : {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            }
        }

        public AmqpType<?, ?> get(int index) {
            switch(index) {
            case 0: {
                return bean.options;
            }
            case 1: {
                return bean.challenge;
            }
            default : {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            }
        }

        public int getListCount() {
            return 2;
        }

        public IAmqpList<AmqpType<?, ?>> getValue() {
            return bean;
        }

        public Iterator<AmqpType<?, ?>> iterator() {
            return new AmqpListIterator<AmqpType<?, ?>>(bean);
        }


        private final void copyCheck() {
            if(buffer != null) {;
                throw new IllegalStateException("unwriteable");
            }
            if(bean != this) {;
                copy(bean);
            }
        }

        private final void copy(AmqpSaslChallenge.AmqpSaslChallengeBean other) {
            bean = this;
        }

        public boolean equals(Object o){
            if(this == o) {
                return true;
            }

            if(o == null || !(o instanceof AmqpSaslChallenge)) {
                return false;
            }

            return equals((AmqpSaslChallenge) o);
        }

        public boolean equals(AmqpSaslChallenge b) {

            if(b.getOptions() == null ^ getOptions() == null) {
                return false;
            }
            if(b.getOptions() != null && !b.getOptions().equals(getOptions())){ 
                return false;
            }

            if(b.getChallenge() == null ^ getChallenge() == null) {
                return false;
            }
            if(b.getChallenge() != null && !b.getChallenge().equals(getChallenge())){ 
                return false;
            }
            return true;
        }

        public int hashCode() {
            return AbstractAmqpList.hashCodeFor(this);
        }
    }

    public static class AmqpSaslChallengeBuffer extends AmqpList.AmqpListBuffer implements AmqpSaslChallenge{

        private AmqpSaslChallengeBean bean;

        protected AmqpSaslChallengeBuffer(Encoded<IAmqpList<AmqpType<?, ?>>> encoded) {
            super(encoded);
        }

        public final void setOptions(AmqpMap options) {
            bean().setOptions(options);
        }

        public final IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>> getOptions() {
            return bean().getOptions();
        }

        public void setChallenge(Buffer challenge) {
            bean().setChallenge(challenge);
        }

        public final void setChallenge(AmqpBinary challenge) {
            bean().setChallenge(challenge);
        }

        public final Buffer getChallenge() {
            return bean().getChallenge();
        }

        public void set(int index, AmqpType<?, ?> value) {
            bean().set(index, value);
        }

        public AmqpType<?, ?> get(int index) {
            return bean().get(index);
        }

        public int getListCount() {
            return bean().getListCount();
        }

        public Iterator<AmqpType<?, ?>> iterator() {
            return bean().iterator();
        }

        public AmqpSaslChallenge.AmqpSaslChallengeBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpSaslChallenge bean() {
            if(bean == null) {
                bean = new AmqpSaslChallenge.AmqpSaslChallengeBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equals(Object o){
            return bean().equals(o);
        }

        public boolean equals(AmqpSaslChallenge o){
            return bean().equals(o);
        }

        public int hashCode() {
            return bean().hashCode();
        }

        public static AmqpSaslChallenge.AmqpSaslChallengeBuffer create(Encoded<IAmqpList<AmqpType<?, ?>>> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpSaslChallenge.AmqpSaslChallengeBuffer(encoded);
        }

        public static AmqpSaslChallenge.AmqpSaslChallengeBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpSaslChallenge(in));
        }

        public static AmqpSaslChallenge.AmqpSaslChallengeBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpSaslChallenge(buffer, offset));
        }
    }
}