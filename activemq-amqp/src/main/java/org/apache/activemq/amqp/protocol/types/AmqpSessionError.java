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
import java.lang.Short;
import java.lang.String;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents a details of a Session error
 * <p>
 * This struct carries information on an exception which has occurred on the Session. The
 * command-id, when given, correlates the error to a specific command.
 * </p>
 */
public interface AmqpSessionError extends AmqpList {



    /**
     * error code indicating the type of error
     */
    public void setErrorCode(AmqpSessionErrorCode errorCode);

    /**
     * error code indicating the type of error
     */
    public AmqpSessionErrorCode getErrorCode();

    /**
     * exceptional command
     * <p>
     * The command-id of the command which caused the exception. If the exception was not
     * caused by a specific command, this value is not set.
     * </p>
     */
    public void setCommandId(AmqpSequenceNo commandId);

    /**
     * exceptional command
     * <p>
     * The command-id of the command which caused the exception. If the exception was not
     * caused by a specific command, this value is not set.
     * </p>
     */
    public AmqpSequenceNo getCommandId();

    /**
     * the class code of the command whose execution gave rise to the error (if appropriate)
     */
    public void setCommandCode(Short commandCode);

    /**
     * the class code of the command whose execution gave rise to the error (if appropriate)
     */
    public void setCommandCode(AmqpUbyte commandCode);

    /**
     * the class code of the command whose execution gave rise to the error (if appropriate)
     */
    public Short getCommandCode();

    /**
     * index of the exceptional field
     * <p>
     * The zero based index of the exceptional field within the arguments to the exceptional
     * command. If the exception was not caused by a specific field, this value is not set.
     * </p>
     */
    public void setFieldIndex(Short fieldIndex);

    /**
     * index of the exceptional field
     * <p>
     * The zero based index of the exceptional field within the arguments to the exceptional
     * command. If the exception was not caused by a specific field, this value is not set.
     * </p>
     */
    public void setFieldIndex(AmqpUbyte fieldIndex);

    /**
     * index of the exceptional field
     * <p>
     * The zero based index of the exceptional field within the arguments to the exceptional
     * command. If the exception was not caused by a specific field, this value is not set.
     * </p>
     */
    public Short getFieldIndex();

    /**
     * descriptive text about the exception
     * <p>
     * The description provided is implementation defined, but MUST be in the language
     * appropriate for the selected locale. The intention is that this description is suitable
     * for logging or alerting output.
     * </p>
     */
    public void setDescription(String description);

    /**
     * descriptive text about the exception
     * <p>
     * The description provided is implementation defined, but MUST be in the language
     * appropriate for the selected locale. The intention is that this description is suitable
     * for logging or alerting output.
     * </p>
     */
    public void setDescription(AmqpString description);

    /**
     * descriptive text about the exception
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

    public static class AmqpSessionErrorBean implements AmqpSessionError{

        private AmqpSessionErrorBuffer buffer;
        private AmqpSessionErrorBean bean = this;
        private AmqpSessionErrorCode errorCode;
        private AmqpSequenceNo commandId;
        private AmqpUbyte commandCode;
        private AmqpUbyte fieldIndex;
        private AmqpString description;
        private AmqpMap errorInfo;

        public AmqpSessionErrorBean() {
        }

        public AmqpSessionErrorBean(IAmqpList value) {
            //TODO we should defer decoding of the described type:
            for(int i = 0; i < value.getListCount(); i++) {
                set(i, value.get(i));
            }
        }

        public AmqpSessionErrorBean(AmqpSessionError.AmqpSessionErrorBean other) {
            this.bean = other;
        }

        public final AmqpSessionErrorBean copy() {
            return new AmqpSessionError.AmqpSessionErrorBean(bean);
        }

        public final AmqpSessionError.AmqpSessionErrorBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            if(buffer == null) {
                buffer = new AmqpSessionErrorBuffer(marshaller.encode(this));
            }
            return buffer;
        }

        public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{
            getBuffer(marshaller).marshal(out, marshaller);
        }


        public final void setErrorCode(AmqpSessionErrorCode errorCode) {
            copyCheck();
            bean.errorCode = errorCode;
        }

        public final AmqpSessionErrorCode getErrorCode() {
            return bean.errorCode;
        }

        public final void setCommandId(AmqpSequenceNo commandId) {
            copyCheck();
            bean.commandId = commandId;
        }

        public final AmqpSequenceNo getCommandId() {
            return bean.commandId;
        }

        public void setCommandCode(Short commandCode) {
            setCommandCode(new AmqpUbyte.AmqpUbyteBean(commandCode));
        }


        public final void setCommandCode(AmqpUbyte commandCode) {
            copyCheck();
            bean.commandCode = commandCode;
        }

        public final Short getCommandCode() {
            return bean.commandCode.getValue();
        }

        public void setFieldIndex(Short fieldIndex) {
            setFieldIndex(new AmqpUbyte.AmqpUbyteBean(fieldIndex));
        }


        public final void setFieldIndex(AmqpUbyte fieldIndex) {
            copyCheck();
            bean.fieldIndex = fieldIndex;
        }

        public final Short getFieldIndex() {
            return bean.fieldIndex.getValue();
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
                setErrorCode(AmqpSessionErrorCode.get((AmqpUshort)value));
                break;
            }
            case 1: {
                setCommandId((AmqpSequenceNo) value);
                break;
            }
            case 2: {
                setCommandCode((AmqpUbyte) value);
                break;
            }
            case 3: {
                setFieldIndex((AmqpUbyte) value);
                break;
            }
            case 4: {
                setDescription((AmqpString) value);
                break;
            }
            case 5: {
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
                return bean.commandId;
            }
            case 2: {
                return bean.commandCode;
            }
            case 3: {
                return bean.fieldIndex;
            }
            case 4: {
                return bean.description;
            }
            case 5: {
                return bean.errorInfo;
            }
            default : {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            }
        }

        public int getListCount() {
            return 6;
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

        private final void copy(AmqpSessionError.AmqpSessionErrorBean other) {
            this.errorCode= other.errorCode;
            this.commandId= other.commandId;
            this.commandCode= other.commandCode;
            this.fieldIndex= other.fieldIndex;
            this.description= other.description;
            this.errorInfo= other.errorInfo;
            bean = this;
        }

        public boolean equivalent(AmqpType<?,?> t){
            if(this == t) {
                return true;
            }

            if(t == null || !(t instanceof AmqpSessionError)) {
                return false;
            }

            return equivalent((AmqpSessionError) t);
        }

        public boolean equivalent(AmqpSessionError b) {

            if(b.getErrorCode() == null ^ getErrorCode() == null) {
                return false;
            }
            if(b.getErrorCode() != null && !b.getErrorCode().equals(getErrorCode())){ 
                return false;
            }

            if(b.getCommandId() == null ^ getCommandId() == null) {
                return false;
            }
            if(b.getCommandId() != null && !b.getCommandId().equals(getCommandId())){ 
                return false;
            }

            if(b.getCommandCode() == null ^ getCommandCode() == null) {
                return false;
            }
            if(b.getCommandCode() != null && !b.getCommandCode().equals(getCommandCode())){ 
                return false;
            }

            if(b.getFieldIndex() == null ^ getFieldIndex() == null) {
                return false;
            }
            if(b.getFieldIndex() != null && !b.getFieldIndex().equals(getFieldIndex())){ 
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

    public static class AmqpSessionErrorBuffer extends AmqpList.AmqpListBuffer implements AmqpSessionError{

        private AmqpSessionErrorBean bean;

        protected AmqpSessionErrorBuffer(Encoded<IAmqpList> encoded) {
            super(encoded);
        }

        public final void setErrorCode(AmqpSessionErrorCode errorCode) {
            bean().setErrorCode(errorCode);
        }

        public final AmqpSessionErrorCode getErrorCode() {
            return bean().getErrorCode();
        }

        public final void setCommandId(AmqpSequenceNo commandId) {
            bean().setCommandId(commandId);
        }

        public final AmqpSequenceNo getCommandId() {
            return bean().getCommandId();
        }

    public void setCommandCode(Short commandCode) {
            bean().setCommandCode(commandCode);
        }

        public final void setCommandCode(AmqpUbyte commandCode) {
            bean().setCommandCode(commandCode);
        }

        public final Short getCommandCode() {
            return bean().getCommandCode();
        }

    public void setFieldIndex(Short fieldIndex) {
            bean().setFieldIndex(fieldIndex);
        }

        public final void setFieldIndex(AmqpUbyte fieldIndex) {
            bean().setFieldIndex(fieldIndex);
        }

        public final Short getFieldIndex() {
            return bean().getFieldIndex();
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

        public AmqpSessionError.AmqpSessionErrorBuffer getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{
            return this;
        }

        protected AmqpSessionError bean() {
            if(bean == null) {
                bean = new AmqpSessionError.AmqpSessionErrorBean(encoded.getValue());
                bean.buffer = this;
            }
            return bean;
        }

        public boolean equivalent(AmqpType<?, ?> t) {
            return bean().equivalent(t);
        }

        public static AmqpSessionError.AmqpSessionErrorBuffer create(Encoded<IAmqpList> encoded) {
            if(encoded.isNull()) {
                return null;
            }
            return new AmqpSessionError.AmqpSessionErrorBuffer(encoded);
        }

        public static AmqpSessionError.AmqpSessionErrorBuffer create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {
            return create(marshaller.unmarshalAmqpSessionError(in));
        }

        public static AmqpSessionError.AmqpSessionErrorBuffer create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {
            return create(marshaller.decodeAmqpSessionError(buffer, offset));
        }
    }
}