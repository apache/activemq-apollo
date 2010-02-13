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
import java.math.BigInteger;
import java.util.Iterator;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a a Message fragment
 * <p>
 * A Message fragment may contain an entire single section Message, an entire section, or an
 * arbitrary fragment of a single section. A fragment cannot contain data from more than one
 * section.
 * </p>
 */
public interface AmqpFragment extends AmqpList {



    /**
     * indicates the fragment is the first in the Section
     * <p>
     * If this flag is true, then the beginning of the payload corresponds with a section
     * boundary within the Message.
     * </p>
     */
    public void setFirst(Boolean first);

    /**
     * indicates the fragment is the first in the Section
     * <p>
     * If this flag is true, then the beginning of the payload corresponds with a section
     * boundary within the Message.
     * </p>
     */
    public void setFirst(boolean first);

    /**
     * indicates the fragment is the first in the Section
     * <p>
     * If this flag is true, then the beginning of the payload corresponds with a section
     * boundary within the Message.
     * </p>
     */
    public void setFirst(AmqpBoolean first);

    /**
     * indicates the fragment is the first in the Section
     * <p>
     * If this flag is true, then the beginning of the payload corresponds with a section
     * boundary within the Message.
     * </p>
     */
    public Boolean getFirst();

    /**
     * indicates the fragment is the last in the Section
     * <p>
     * If this flag is true, then the end of the payload corresponds with a section boundary
     * within the Message.
     * </p>
     */
    public void setLast(Boolean last);

    /**
     * indicates the fragment is the last in the Section
     * <p>
     * If this flag is true, then the end of the payload corresponds with a section boundary
     * within the Message.
     * </p>
     */
    public void setLast(boolean last);

    /**
     * indicates the fragment is the last in the Section
     * <p>
     * If this flag is true, then the end of the payload corresponds with a section boundary
     * within the Message.
     * </p>
     */
    public void setLast(AmqpBoolean last);

    /**
     * indicates the fragment is the last in the Section
     * <p>
     * If this flag is true, then the end of the payload corresponds with a section boundary
     * within the Message.
     * </p>
     */
    public Boolean getLast();

    /**
     * indicates the format of the Message section
     * <p>
     * The format code indicates the format of the current section of the Message. A Message
     * may have multiple sections, and therefore multiple format codes, however the format code
     * is only permitted to change at section boundaries.
     * </p>
     */
    public void setFormatCode(Long formatCode);

    /**
     * indicates the format of the Message section
     * <p>
     * The format code indicates the format of the current section of the Message. A Message
     * may have multiple sections, and therefore multiple format codes, however the format code
     * is only permitted to change at section boundaries.
     * </p>
     */
    public void setFormatCode(long formatCode);

    /**
     * indicates the format of the Message section
     * <p>
     * The format code indicates the format of the current section of the Message. A Message
     * may have multiple sections, and therefore multiple format codes, however the format code
     * is only permitted to change at section boundaries.
     * </p>
     */
    public void setFormatCode(AmqpUint formatCode);

    /**
     * indicates the format of the Message section
     * <p>
     * The format code indicates the format of the current section of the Message. A Message
     * may have multiple sections, and therefore multiple format codes, however the format code
     * is only permitted to change at section boundaries.
     * </p>
     */
    public Long getFormatCode();

    /**
     * the payload offset within the Message
     */
    public void setFragmentOffset(BigInteger fragmentOffset);

    /**
     * the payload offset within the Message
     */
    public void setFragmentOffset(AmqpUlong fragmentOffset);

    /**
     * the payload offset within the Message
     */
    public BigInteger getFragmentOffset();

    /**
     * Message data
     */
    public void setPayload(Buffer payload);

    /**
     * Message data
     */
    public void setPayload(AmqpBinary payload);

    /**
     * Message data
     */
    public Buffer getPayload();

    public static class AmqpFragmentBean implements AmqpFragment{

        private AmqpFragmentBuffer buffer;
        private AmqpFragmentBean bean = this;
        private AmqpBoolean first;
        private AmqpBoolean last;
        private AmqpUint formatCode;
        private AmqpUlong fragmentOffset;
        private AmqpBinary payload;

        AmqpFragmentBean() {
        }

        AmqpFragmentBean(IAmqpList<AmqpType<?, ?>> value) {

            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        AmqpFragmentBean(AmqpFragment.AmqpFragmentBean other) {
            this.bean = other;
        }

        public final AmqpFragmentBean copy() {
            return new AmqpFragment.AmqpFragmentBean(bean);
        }

        public final AmqpFragment.AmqpFragmentBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpFragmentBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public void setFirst(Boolean first) {
            setFirst(TypeFactory.createAmqpBoolean(first));
        }


        public void setFirst(boolean first) {
            setFirst(TypeFactory.createAmqpBoolean(first));
        }


        public final void setFirst(AmqpBoolean first) {
            copyCheck();
            bean.first = first;
        }

        public final Boolean getFirst() {
            return bean.first.getValue();
        }

        public void setLast(Boolean last) {
            setLast(TypeFactory.createAmqpBoolean(last));
        }


        public void setLast(boolean last) {
            setLast(TypeFactory.createAmqpBoolean(last));
        }


        public final void setLast(AmqpBoolean last) {
            copyCheck();
            bean.last = last;
        }

        public final Boolean getLast() {
            return bean.last.getValue();
        }

        public void setFormatCode(Long formatCode) {
            setFormatCode(TypeFactory.createAmqpUint(formatCode));
        }


        public void setFormatCode(long formatCode) {
            setFormatCode(TypeFactory.createAmqpUint(formatCode));
        }


        public final void setFormatCode(AmqpUint formatCode) {
            copyCheck();
            bean.formatCode = formatCode;
        }

        public final Long getFormatCode() {
            return bean.formatCode.getValue();
        }

        public void setFragmentOffset(BigInteger fragmentOffset) {
            setFragmentOffset(TypeFactory.createAmqpUlong(fragmentOffset));
        }


        public final void setFragmentOffset(AmqpUlong fragmentOffset) {
            copyCheck();
            bean.fragmentOffset = fragmentOffset;
        }

        public final BigInteger getFragmentOffset() {
            return bean.fragmentOffset.getValue();
        }

        public void setPayload(Buffer payload) {
            setPayload(TypeFactory.createAmqpBinary(payload));
        }


        public final void setPayload(AmqpBinary payload) {
            copyCheck();
            bean.payload = payload;
        }

        public final Buffer getPayload() {
            return bean.payload.getValue();
        }

        public void set(int index, AmqpType<?, ?> value) {
            switch(index) {
            case 0: {
                setFirst((AmqpBoolean) value);
                break;
            }
            case 1: {
                setLast((AmqpBoolean) value);
                break;
            }
            case 2: {
                setFormatCode((AmqpUint) value);
                break;
            }
            case 3: {
                setFragmentOffset((AmqpUlong) value);
                break;
            }
            case 4: {
                setPayload((AmqpBinary) value);
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
                return bean.first;
            }
            case 1: {
                return bean.last;
            }
            case 2: {
                return bean.formatCode;
            }
            case 3: {
                return bean.fragmentOffset;
            }
            case 4: {
                return bean.payload;
            }
            default : {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            }
        }

        public int getListCount() {
            return 5;
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

        private final void copy(AmqpFragment.AmqpFragmentBean other) {
            bean = this;
        }

        public boolean equals(Object o){
            if(this == o) {
                return true;
            }

            if(o == null || !(o instanceof AmqpFragment)) {
                return false;
            }

            return equals((AmqpFragment) o);
        }

        public boolean equals(AmqpFragment b) {

            if(b.getFirst() == null ^ getFirst() == null) {
                return false;
            }
            if(b.getFirst() != null && !b.getFirst().equals(getFirst())){ 
                return false;
            }

            if(b.getLast() == null ^ getLast() == null) {
                return false;
            }
            if(b.getLast() != null && !b.getLast().equals(getLast())){ 
                return false;
            }

            if(b.getFormatCode() == null ^ getFormatCode() == null) {
                return false;
            }
            if(b.getFormatCode() != null && !b.getFormatCode().equals(getFormatCode())){ 
                return false;
            }

            if(b.getFragmentOffset() == null ^ getFragmentOffset() == null) {
                return false;
            }
            if(b.getFragmentOffset() != null && !b.getFragmentOffset().equals(getFragmentOffset())){ 
                return false;
            }

            if(b.getPayload() == null ^ getPayload() == null) {
                return false;
            }
            if(b.getPayload() != null && !b.getPayload().equals(getPayload())){ 
                return false;
            }
            return true;
        }

        public int hashCode() {
            return AbstractAmqpList.hashCodeFor(this);
        }
    }

    public static class AmqpFragmentBuffer extends AmqpList.AmqpListBuffer implements AmqpFragment{

        private AmqpFragmentBean bean;

        protected AmqpFragmentBuffer(Encoded<IAmqpList<AmqpType<?, ?>>> encoded) {
            super(encoded);
        }

        public void setFirst(Boolean first) {
            bean().setFirst(first);
        }

        public void setFirst(boolean first) {
            bean().setFirst(first);
        }


        public final void setFirst(AmqpBoolean first) {
            bean().setFirst(first);
        }

        public final Boolean getFirst() {
            return bean().getFirst();
        }

        public void setLast(Boolean last) {
            bean().setLast(last);
        }

        public void setLast(boolean last) {
            bean().setLast(last);
        }


        public final void setLast(AmqpBoolean last) {
            bean().setLast(last);
        }

        public final Boolean getLast() {
            return bean().getLast();
        }

        public void setFormatCode(Long formatCode) {
            bean().setFormatCode(formatCode);
        }

        public void setFormatCode(long formatCode) {
            bean().setFormatCode(formatCode);
        }


        public final void setFormatCode(AmqpUint formatCode) {
            bean().setFormatCode(formatCode);
        }

        public final Long getFormatCode() {
            return bean().getFormatCode();
        }

        public void setFragmentOffset(BigInteger fragmentOffset) {
            bean().setFragmentOffset(fragmentOffset);
        }

        public final void setFragmentOffset(AmqpUlong fragmentOffset) {
            bean().setFragmentOffset(fragmentOffset);
        }

        public final BigInteger getFragmentOffset() {
            return bean().getFragmentOffset();
        }

        public void setPayload(Buffer payload) {
            bean().setPayload(payload);
        }

        public final void setPayload(AmqpBinary payload) {
            bean().setPayload(payload);
        }

        public final Buffer getPayload() {
            return bean().getPayload();
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

        public AmqpFragment.AmqpFragmentBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpFragment bean() {
            if(bean == null) {
                bean = new AmqpFragment.AmqpFragmentBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equals(Object o){
            return bean().equals(o);
        }

        public boolean equals(AmqpFragment o){
            return bean().equals(o);
        }

        public int hashCode() {
            return bean().hashCode();
        }

        public static AmqpFragment.AmqpFragmentBuffer create(Encoded<IAmqpList<AmqpType<?, ?>>> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpFragment.AmqpFragmentBuffer(encoded);
        }

        public static AmqpFragment.AmqpFragmentBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpFragment(in));
        }

        public static AmqpFragment.AmqpFragmentBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpFragment(buffer, offset));
        }
    }
}