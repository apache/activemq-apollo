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
import java.util.Iterator;
import org.apache.activemq.amqp.protocol.AmqpCommand;
import org.apache.activemq.amqp.protocol.AmqpCommandHandler;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a re-establish Link parameters
 * <p>
 * The relink command updates the source and/or target definition(s) associated with an
 * existing link. This is the same as unlinking and relinking except you will not receive
 * messages already successfully transferred along the link. Relinking will not always be
 * possible, e.g. if the new and the old source refer to distinct nodes. If a relink is not
 * possible, the receiver MUST unlink the link with an appropriate error before signaling the
 * relink as executed.
 * </p>
 */
public interface AmqpRelink extends AmqpList, AmqpCommand {



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
     * the source for Messages
     */
    public void setSource(HashMap<AmqpType<?,?>, AmqpType<?,?>> source);

    /**
     * the source for Messages
     */
    public void setSource(AmqpMap source);

    /**
     * the source for Messages
     */
    public HashMap<AmqpType<?,?>, AmqpType<?,?>> getSource();

    /**
     * the target for Messages
     */
    public void setTarget(HashMap<AmqpType<?,?>, AmqpType<?,?>> target);

    /**
     * the target for Messages
     */
    public void setTarget(AmqpMap target);

    /**
     * the target for Messages
     */
    public HashMap<AmqpType<?,?>, AmqpType<?,?>> getTarget();

    public static class AmqpRelinkBean implements AmqpRelink{

        private AmqpRelinkBuffer buffer;
        private AmqpRelinkBean bean = this;
        private AmqpOptions options;
        private AmqpHandle handle;
        private AmqpMap source;
        private AmqpMap target;

        public AmqpRelinkBean() {
        }

        public AmqpRelinkBean(IAmqpList value) {
            //TODO we should defer decoding of the described type:
            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        public AmqpRelinkBean(AmqpRelink.AmqpRelinkBean other) {
            this.bean = other;
        }

        public final AmqpRelinkBean copy() {
            return new AmqpRelink.AmqpRelinkBean(bean);
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleRelink(this);
        }

        public final AmqpRelink.AmqpRelinkBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpRelinkBuffer(marshaller.encode(this));
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

        public void setSource(HashMap<AmqpType<?,?>, AmqpType<?,?>> source) {
            setSource(new AmqpMap.AmqpMapBean(source));
        }


        public final void setSource(AmqpMap source) {
            copyCheck();
            bean.source = source;
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getSource() {
            return bean.source.getValue();
        }

        public void setTarget(HashMap<AmqpType<?,?>, AmqpType<?,?>> target) {
            setTarget(new AmqpMap.AmqpMapBean(target));
        }


        public final void setTarget(AmqpMap target) {
            copyCheck();
            bean.target = target;
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getTarget() {
            return bean.target.getValue();
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
                setSource((AmqpMap) value);
                break;
            }
            case 3: {
                setTarget((AmqpMap) value);
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
                return bean.source;
            }
            case 3: {
                return bean.target;
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

        private final void copy(AmqpRelink.AmqpRelinkBean other) {
            this.options= other.options;
            this.handle= other.handle;
            this.source= other.source;
            this.target= other.target;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpRelink)) {
                return false;
            }

            return equivalent((AmqpRelink) t);
        }

        public boolean equivalent(AmqpRelink b) {

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

            if(b.getSource() == null ^ getSource() == null) {
                return false;
            }
            if(b.getSource() != null && !b.getSource().equals(getSource())){ 
                return false;
            }

            if(b.getTarget() == null ^ getTarget() == null) {
                return false;
            }
            if(b.getTarget() != null && !b.getTarget().equals(getTarget())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpRelinkBuffer extends AmqpList.AmqpListBuffer implements AmqpRelink{

        private AmqpRelinkBean bean;

        protected AmqpRelinkBuffer(Encoded<IAmqpList> encoded) {
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

    public void setSource(HashMap<AmqpType<?,?>, AmqpType<?,?>> source) {
            bean().setSource(source);
        }

        public final void setSource(AmqpMap source) {
            bean().setSource(source);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getSource() {
            return bean().getSource();
        }

    public void setTarget(HashMap<AmqpType<?,?>, AmqpType<?,?>> target) {
            bean().setTarget(target);
        }

        public final void setTarget(AmqpMap target) {
            bean().setTarget(target);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getTarget() {
            return bean().getTarget();
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

        public AmqpRelink.AmqpRelinkBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpRelink bean() {
            if(bean == null) {
                bean = new AmqpRelink.AmqpRelinkBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public final void handle(AmqpCommandHandler handler) throws Exception {
            handler.handleRelink(this);
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpRelink.AmqpRelinkBuffer create(Encoded<IAmqpList> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpRelink.AmqpRelinkBuffer(encoded);
        }

        public static AmqpRelink.AmqpRelinkBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpRelink(in));
        }

        public static AmqpRelink.AmqpRelinkBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpRelink(buffer, offset));
        }
    }
}