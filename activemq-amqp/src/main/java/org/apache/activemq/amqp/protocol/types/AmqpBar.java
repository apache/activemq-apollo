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
 * Represents a establish a barrier for Message acknowledgments on a Link
 * <p>
 * Messages transferred along this Link from the Message specified in the bar command and
 * onwards will remain unacknowledged despite Session level acknowledgments that include
 * them.
 * </p>
 */
public interface AmqpBar extends AmqpList, AmqpCommand {



    /**
     * options map
     */
    public void setOptions(AmqpOptions options);

    /**
     * options map
     */
    public AmqpOptions getOptions();

    /**
     * <p>
     * Specifies the Link to which the barrier applies.
     * </p>
     */
    public void setHandle(AmqpHandle handle);

    /**
     * <p>
     * Specifies the Link to which the barrier applies.
     * </p>
     */
    public AmqpHandle getHandle();

    /**
     * <p>
     * If the barrier field is not set then the barrier is removed.
     * </p>
     */
    public void setBarrier(AmqpDeliveryTag barrier);

    /**
     * <p>
     * If the barrier field is not set then the barrier is removed.
     * </p>
     */
    public AmqpDeliveryTag getBarrier();

    public static class AmqpBarBean implements AmqpBar{

        private AmqpBarBuffer buffer;
        private AmqpBarBean bean = this;
        private AmqpOptions options;
        private AmqpHandle handle;
        private AmqpDeliveryTag barrier;

        public AmqpBarBean() {
        }

        public AmqpBarBean(IAmqpList value) {
            //TODO we should defer decoding of the described type:
            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        public AmqpBarBean(AmqpBar.AmqpBarBean other) {
            this.bean = other;
        }

        public final AmqpBarBean copy() {
            return new AmqpBar.AmqpBarBean(bean);
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleBar(this);
        }

        public final AmqpBar.AmqpBarBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpBarBuffer(marshaller.encode(this));
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

        public final void setBarrier(AmqpDeliveryTag barrier) {
            copyCheck();
            bean.barrier = barrier;
        }

        public final AmqpDeliveryTag getBarrier() {
            return bean.barrier;
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
                setBarrier((AmqpDeliveryTag) value);
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
                return bean.barrier;
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

        private final void copy(AmqpBar.AmqpBarBean other) {
            this.options= other.options;
            this.handle= other.handle;
            this.barrier= other.barrier;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpBar)) {
                return false;
            }

            return equivalent((AmqpBar) t);
        }

        public boolean equivalent(AmqpBar b) {

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

            if(b.getBarrier() == null ^ getBarrier() == null) {
                return false;
            }
            if(b.getBarrier() != null && !b.getBarrier().equals(getBarrier())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpBarBuffer extends AmqpList.AmqpListBuffer implements AmqpBar{

        private AmqpBarBean bean;

        protected AmqpBarBuffer(Encoded<IAmqpList> encoded) {
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

        public final void setBarrier(AmqpDeliveryTag barrier) {
            bean().setBarrier(barrier);
        }

        public final AmqpDeliveryTag getBarrier() {
            return bean().getBarrier();
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

        public AmqpBar.AmqpBarBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpBar bean() {
            if(bean == null) {
                bean = new AmqpBar.AmqpBarBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleBar(this);
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpBar.AmqpBarBuffer create(Encoded<IAmqpList> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpBar.AmqpBarBuffer(encoded);
        }

        public static AmqpBar.AmqpBarBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpBar(in));
        }

        public static AmqpBar.AmqpBarBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpBar(buffer, offset));
        }
    }
}