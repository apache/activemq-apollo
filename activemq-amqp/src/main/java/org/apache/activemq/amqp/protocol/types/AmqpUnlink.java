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
import org.apache.activemq.amqp.protocol.AmqpCommand;
import org.apache.activemq.amqp.protocol.AmqpCommandHandler;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a close the Link
 * <p>
 * Close the Link and un-map the handle.
 * </p>
 */
public interface AmqpUnlink extends AmqpList, AmqpCommand {



    /**
     * options map
     */
    public void setOptions(AmqpOptions options);

    /**
     * options map
     */
    public AmqpOptions getOptions();

    /**
     * identifies the Link
     */
    public void setHandle(AmqpHandle handle);

    /**
     * identifies the Link
     */
    public AmqpHandle getHandle();

    /**
     * error causing the unlink
     * <p>
     * If set, this field indicates that the Link is being unlinked due to an exceptional
     * condition. The value of the field should contain details on the cause of the exception.
     * </p>
     */
    public void setException(AmqpLinkError exception);

    /**
     * error causing the unlink
     * <p>
     * If set, this field indicates that the Link is being unlinked due to an exceptional
     * condition. The value of the field should contain details on the cause of the exception.
     * </p>
     */
    public AmqpLinkError getException();

    public static class AmqpUnlinkBean implements AmqpUnlink{

        private AmqpUnlinkBuffer buffer;
        private AmqpUnlinkBean bean = this;
        private AmqpOptions options;
        private AmqpHandle handle;
        private AmqpLinkError exception;

        public AmqpUnlinkBean() {
        }

        public AmqpUnlinkBean(IAmqpList value) {
            //TODO we should defer decoding of the described type:
            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        public AmqpUnlinkBean(AmqpUnlink.AmqpUnlinkBean other) {
            this.bean = other;
        }

        public final AmqpUnlinkBean copy() {
            return new AmqpUnlink.AmqpUnlinkBean(bean);
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleUnlink(this);
        }

        public final AmqpUnlink.AmqpUnlinkBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpUnlinkBuffer(marshaller.encode(this));
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

        public final void setHandle(AmqpHandle handle) {
            copyCheck();
            bean.handle = handle;
        }

        public final AmqpHandle getHandle() {
            return bean.handle;
        }

        public final void setException(AmqpLinkError exception) {
            copyCheck();
            bean.exception = exception;
        }

        public final AmqpLinkError getException() {
            return bean.exception;
        }

        public void set(int index, AmqpType<?, ?> value) {
            switch(index) {
            case 0: {
                setOptions((AmqpOptions) value);
                break;
            }
            case 1: {
                setHandle((AmqpHandle) value);
                break;
            }
            case 2: {
                setException((AmqpLinkError) value);
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
                return bean.handle;
            }
            case 2: {
                return bean.exception;
            }
            default : {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            }
        }

        public int getListCount() {
            return 3;
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

        private final void copy(AmqpUnlink.AmqpUnlinkBean other) {
            this.options= other.options;
            this.handle= other.handle;
            this.exception= other.exception;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpUnlink)) {
                return false;
            }

            return equivalent((AmqpUnlink) t);
        }

        public boolean equivalent(AmqpUnlink b) {

            if(b.getOptions() == null ^ getOptions() == null) {
                return false;
            }
            if(b.getOptions() != null && !b.getOptions().equals(getOptions())){ 
                return false;
            }

            if(b.getHandle() == null ^ getHandle() == null) {
                return false;
            }
            if(b.getHandle() != null && !b.getHandle().equals(getHandle())){ 
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

    public static class AmqpUnlinkBuffer extends AmqpList.AmqpListBuffer implements AmqpUnlink{

        private AmqpUnlinkBean bean;

        protected AmqpUnlinkBuffer(Encoded<IAmqpList> encoded) {
            super(encoded);
        }

        public final void setOptions(AmqpOptions options) {
            bean().setOptions(options);
        }

        public final AmqpOptions getOptions() {
            return bean().getOptions();
        }

        public final void setHandle(AmqpHandle handle) {
            bean().setHandle(handle);
        }

        public final AmqpHandle getHandle() {
            return bean().getHandle();
        }

        public final void setException(AmqpLinkError exception) {
            bean().setException(exception);
        }

        public final AmqpLinkError getException() {
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

        public AmqpUnlink.AmqpUnlinkBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpUnlink bean() {
            if(bean == null) {
                bean = new AmqpUnlink.AmqpUnlinkBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleUnlink(this);
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpUnlink.AmqpUnlinkBuffer create(Encoded<IAmqpList> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpUnlink.AmqpUnlinkBuffer(encoded);
        }

        public static AmqpUnlink.AmqpUnlinkBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpUnlink(in));
        }

        public static AmqpUnlink.AmqpUnlinkBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpUnlink(buffer, offset));
        }
    }
}