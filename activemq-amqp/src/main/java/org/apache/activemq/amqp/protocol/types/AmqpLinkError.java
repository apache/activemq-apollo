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
import java.lang.String;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a details of a Link error
 */
public interface AmqpLinkError extends AmqpList {



    /**
     * Link error code
     * <p>
     * A numeric code indicating the reason for the Link unlink.
     * </p>
     */
    public void setErrorCode(AmqpLinkErrorCode errorCode);

    /**
     * Link error code
     * <p>
     * A numeric code indicating the reason for the Link unlink.
     * </p>
     */
    public AmqpLinkErrorCode getErrorCode();

    /**
     * description text about the exception
     * <p>
     * The description provided is implementation defined, but MUST be in the language
     * appropriate for the selected locale. The intention is that this description is suitable
     * for logging or alerting output.
     * </p>
     */
    public void setDescription(String description);

    /**
     * description text about the exception
     * <p>
     * The description provided is implementation defined, but MUST be in the language
     * appropriate for the selected locale. The intention is that this description is suitable
     * for logging or alerting output.
     * </p>
     */
    public void setDescription(AmqpString description);

    /**
     * description text about the exception
     * <p>
     * The description provided is implementation defined, but MUST be in the language
     * appropriate for the selected locale. The intention is that this description is suitable
     * for logging or alerting output.
     * </p>
     */
    public String getDescription();

    /**
     * map to carry additional information about the error
     */
    public void setErrorInfo(HashMap<AmqpType<?,?>, AmqpType<?,?>> errorInfo);

    /**
     * map to carry additional information about the error
     */
    public void setErrorInfo(AmqpMap errorInfo);

    /**
     * map to carry additional information about the error
     */
    public HashMap<AmqpType<?,?>, AmqpType<?,?>> getErrorInfo();

    public static class AmqpLinkErrorBean implements AmqpLinkError{

        private AmqpLinkErrorBuffer buffer;
        private AmqpLinkErrorBean bean = this;
        private AmqpLinkErrorCode errorCode;
        private AmqpString description;
        private AmqpMap errorInfo;

        public AmqpLinkErrorBean() {
        }

        public AmqpLinkErrorBean(IAmqpList value) {
            //TODO we should defer decoding of the described type:
            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        public AmqpLinkErrorBean(AmqpLinkError.AmqpLinkErrorBean other) {
            this.bean = other;
        }

        public final AmqpLinkErrorBean copy() {
            return new AmqpLinkError.AmqpLinkErrorBean(bean);
        }

        public final AmqpLinkError.AmqpLinkErrorBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpLinkErrorBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public final void setErrorCode(AmqpLinkErrorCode errorCode) {
            copyCheck();
            bean.errorCode = errorCode;
        }

        public final AmqpLinkErrorCode getErrorCode() {
            return bean.errorCode;
        }

        public void setDescription(String description) {
            setDescription(new AmqpString.AmqpStringBean(description));
        }


        public final void setDescription(AmqpString description) {
            copyCheck();
            bean.description = description;
        }

        public final String getDescription() {
            return bean.description.getValue();
        }

        public void setErrorInfo(HashMap<AmqpType<?,?>, AmqpType<?,?>> errorInfo) {
            setErrorInfo(new AmqpMap.AmqpMapBean(errorInfo));
        }


        public final void setErrorInfo(AmqpMap errorInfo) {
            copyCheck();
            bean.errorInfo = errorInfo;
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getErrorInfo() {
            return bean.errorInfo.getValue();
        }

        public void set(int index, AmqpType<?, ?> value) {
            switch(index) {
            case 0: {
                setErrorCode(AmqpLinkErrorCode.get((AmqpUshort)value));
                break;
            }
            case 1: {
                setDescription((AmqpString) value);
                break;
            }
            case 2: {
                setErrorInfo((AmqpMap) value);
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
                if(errorCode == null) {
                    return null;
                }
                return errorCode.getValue();
            }
            case 1: {
                return bean.description;
            }
            case 2: {
                return bean.errorInfo;
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

        private final void copy(AmqpLinkError.AmqpLinkErrorBean other) {
            this.errorCode= other.errorCode;
            this.description= other.description;
            this.errorInfo= other.errorInfo;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpLinkError)) {
                return false;
            }

            return equivalent((AmqpLinkError) t);
        }

        public boolean equivalent(AmqpLinkError b) {

            if(b.getErrorCode() == null ^ getErrorCode() == null) {
                return false;
            }
            if(b.getErrorCode() != null && !b.getErrorCode().equals(getErrorCode())){ 
                return false;
            }

            if(b.getDescription() == null ^ getDescription() == null) {
                return false;
            }
            if(b.getDescription() != null && !b.getDescription().equals(getDescription())){ 
                return false;
            }

            if(b.getErrorInfo() == null ^ getErrorInfo() == null) {
                return false;
            }
            if(b.getErrorInfo() != null && !b.getErrorInfo().equals(getErrorInfo())){ 
                return false;
            }
            return true;
        }
    }

    public static class AmqpLinkErrorBuffer extends AmqpList.AmqpListBuffer implements AmqpLinkError{

        private AmqpLinkErrorBean bean;

        protected AmqpLinkErrorBuffer(Encoded<IAmqpList> encoded) {
            super(encoded);
        }

        public final void setErrorCode(AmqpLinkErrorCode errorCode) {
            bean().setErrorCode(errorCode);
        }

        public final AmqpLinkErrorCode getErrorCode() {
            return bean().getErrorCode();
        }

    public void setDescription(String description) {
            bean().setDescription(description);
        }

        public final void setDescription(AmqpString description) {
            bean().setDescription(description);
        }

        public final String getDescription() {
            return bean().getDescription();
        }

    public void setErrorInfo(HashMap<AmqpType<?,?>, AmqpType<?,?>> errorInfo) {
            bean().setErrorInfo(errorInfo);
        }

        public final void setErrorInfo(AmqpMap errorInfo) {
            bean().setErrorInfo(errorInfo);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> getErrorInfo() {
            return bean().getErrorInfo();
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

        public AmqpLinkError.AmqpLinkErrorBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpLinkError bean() {
            if(bean == null) {
                bean = new AmqpLinkError.AmqpLinkErrorBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpLinkError.AmqpLinkErrorBuffer create(Encoded<IAmqpList> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpLinkError.AmqpLinkErrorBuffer(encoded);
        }

        public static AmqpLinkError.AmqpLinkErrorBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpLinkError(in));
        }

        public static AmqpLinkError.AmqpLinkErrorBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpLinkError(buffer, offset));
        }
    }
}