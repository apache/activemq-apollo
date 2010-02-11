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
import java.util.Iterator;
import org.apache.activemq.amqp.protocol.AmqpCommand;
import org.apache.activemq.amqp.protocol.AmqpCommandHandler;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a detach from the named Session
 * <p>
 * Indicates that the endpoint is being detached from the Connection.
 * </p>
 */
public interface AmqpDetach extends AmqpList, AmqpCommand {



    /**
     * options map
     */
    public void setOptions(AmqpOptions options);

    /**
     * options map
     */
    public AmqpOptions getOptions();

    /**
     * the Session name
     * <p>
     * Identifies the detaching Session.
     * </p>
     */
    public void setName(AmqpSessionName name);

    /**
     * the Session name
     * <p>
     * Identifies the detaching Session.
     * </p>
     */
    public AmqpSessionName getName();

    /**
     * <p>
     * This field, if set, indicates that the Session endpoint will be destroyed when fully
     * detached.
     * </p>
     */
    public void setClosing(Boolean closing);

    /**
     * <p>
     * This field, if set, indicates that the Session endpoint will be destroyed when fully
     * detached.
     * </p>
     */
    public void setClosing(AmqpBoolean closing);

    /**
     * <p>
     * This field, if set, indicates that the Session endpoint will be destroyed when fully
     * detached.
     * </p>
     */
    public Boolean getClosing();

    /**
     * error causing the detach
     * <p>
     * If set, this field indicates that the Session is being detached due to an exceptional
     * condition. The value of the field should contain details on the cause of the exception.
     * </p>
     */
    public void setException(AmqpSessionError exception);

    /**
     * error causing the detach
     * <p>
     * If set, this field indicates that the Session is being detached due to an exceptional
     * condition. The value of the field should contain details on the cause of the exception.
     * </p>
     */
    public AmqpSessionError getException();

    public static class AmqpDetachBean implements AmqpDetach{

        private AmqpDetachBuffer buffer;
        private AmqpDetachBean bean = this;
        private AmqpOptions options;
        private AmqpSessionName name;
        private AmqpBoolean closing;
        private AmqpSessionError exception;

        public AmqpDetachBean() {
        }

        public AmqpDetachBean(IAmqpList value) {
            //TODO we should defer decoding of the described type:
            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        public AmqpDetachBean(AmqpDetach.AmqpDetachBean other) {
            this.bean = other;
        }

        public final AmqpDetachBean copy() {
            return new AmqpDetach.AmqpDetachBean(bean);
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleDetach(this);
        }

        public final AmqpDetach.AmqpDetachBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpDetachBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public final void setOptions(AmqpOptions options) {
            copyCheck();
            bean.options = options;
        }

        public final AmqpOptions getOptions() {
            return bean.options;
        }

        public final void setName(AmqpSessionName name) {
            copyCheck();
            bean.name = name;
        }

        public final AmqpSessionName getName() {
            return bean.name;
        }

        public void setClosing(Boolean closing) {
            setClosing(new AmqpBoolean.AmqpBooleanBean(closing));
        }


        public final void setClosing(AmqpBoolean closing) {
            copyCheck();
            bean.closing = closing;
        }

        public final Boolean getClosing() {
            return bean.closing.getValue();
        }

        public final void setException(AmqpSessionError exception) {
            copyCheck();
            bean.exception = exception;
        }

        public final AmqpSessionError getException() {
            return bean.exception;
        }

        public void set(int index, AmqpType<?, ?> value) {
            switch(index) {
            case 0: {
                setOptions((AmqpOptions) value);
                break;
            }
            case 1: {
                setName((AmqpSessionName) value);
                break;
            }
            case 2: {
                setClosing((AmqpBoolean) value);
                break;
            }
            case 3: {
                setException((AmqpSessionError) value);
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
                return bean.name;
            }
            case 2: {
                return bean.closing;
            }
            case 3: {
                return bean.exception;
            }
            default : {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            }
        }

        public int getListCount() {
            return 4;
        }

        public IAmqpList getValue() {
            return bean;
        }

        public Iterator<AmqpType<?, ?>> iterator() {
            return new AmqpListIterator(bean);
        }


        private final void copyCheck() {
            if(buffer != null) {;
                throw new IllegalStateException("unwriteable");
            }
            if(bean != this) {;
                copy(bean);
            }
        }

        private final void copy(AmqpDetach.AmqpDetachBean other) {
            this.options= other.options;
            this.name= other.name;
            this.closing= other.closing;
            this.exception= other.exception;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpDetach)) {
                return false;
            }

            return equivalent((AmqpDetach) t);
        }

        public boolean equivalent(AmqpDetach b) {

            if(b.getOptions() == null ^ getOptions() == null) {
                return false;
            }
            if(b.getOptions() != null && !b.getOptions().equals(getOptions())){ 
                return false;
            }

            if(b.getName() == null ^ getName() == null) {
                return false;
            }
            if(b.getName() != null && !b.getName().equals(getName())){ 
                return false;
            }

            if(b.getClosing() == null ^ getClosing() == null) {
                return false;
            }
            if(b.getClosing() != null && !b.getClosing().equals(getClosing())){ 
                return false;
            }

            if(b.getException() == null ^ getException() == null) {
                return false;
            }
            if(b.getException() != null && !b.getException().equivalent(getException())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpDetachBuffer extends AmqpList.AmqpListBuffer implements AmqpDetach{

        private AmqpDetachBean bean;

        protected AmqpDetachBuffer(Encoded<IAmqpList> encoded) {
            super(encoded);
        }

        public final void setOptions(AmqpOptions options) {
            bean().setOptions(options);
        }

        public final AmqpOptions getOptions() {
            return bean().getOptions();
        }

        public final void setName(AmqpSessionName name) {
            bean().setName(name);
        }

        public final AmqpSessionName getName() {
            return bean().getName();
        }

    public void setClosing(Boolean closing) {
            bean().setClosing(closing);
        }

        public final void setClosing(AmqpBoolean closing) {
            bean().setClosing(closing);
        }

        public final Boolean getClosing() {
            return bean().getClosing();
        }

        public final void setException(AmqpSessionError exception) {
            bean().setException(exception);
        }

        public final AmqpSessionError getException() {
            return bean().getException();
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

        public IAmqpList getValue() {
            return bean().getValue();
        }

        public AmqpDetach.AmqpDetachBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpDetach bean() {
            if(bean == null) {
                bean = new AmqpDetach.AmqpDetachBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleDetach(this);
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpDetach.AmqpDetachBuffer create(Encoded<IAmqpList> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpDetach.AmqpDetachBuffer(encoded);
        }

        public static AmqpDetach.AmqpDetachBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpDetach(in));
        }

        public static AmqpDetach.AmqpDetachBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpDetach(buffer, offset));
        }
    }
}