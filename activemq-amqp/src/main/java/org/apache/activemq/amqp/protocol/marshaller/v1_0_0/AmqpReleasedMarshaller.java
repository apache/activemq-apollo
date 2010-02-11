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
package org.apache.activemq.amqp.protocol.marshaller.v1_0_0;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.marshaller.UnexpectedTypeException;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder.*;
import org.apache.activemq.amqp.protocol.types.AmqpBoolean;
import org.apache.activemq.amqp.protocol.types.AmqpMessageAttributes;
import org.apache.activemq.amqp.protocol.types.AmqpReleased;
import org.apache.activemq.amqp.protocol.types.AmqpSymbol;
import org.apache.activemq.amqp.protocol.types.AmqpType;
import org.apache.activemq.amqp.protocol.types.AmqpUlong;
import org.apache.activemq.util.buffer.Buffer;

public class AmqpReleasedMarshaller implements DescribedTypeMarshaller<AmqpReleased>{

    static final AmqpReleasedMarshaller SINGLETON = new AmqpReleasedMarshaller();
    private static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> NULL_ENCODED = new Encoder.NullEncoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>>();

    public static final String SYMBOLIC_ID = "amqp:released:map";
    //Format code: 0x00000001:0x00009804:
    public static final long CATEGORY = 1;
    public static final long DESCRIPTOR_ID = 38916;
    public static final long NUMERIC_ID = CATEGORY << 32 | DESCRIPTOR_ID; //(4295006212L)
    //Hard coded descriptor:
    public static final EncodedBuffer DESCRIPTOR = FormatCategory.createBuffer(new Buffer(new byte [] {
        (byte) 0x80,                                         // ulong descriptor encoding)
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,  // CATEGORY CODE
        (byte) 0x00, (byte) 0x00, (byte) 0x98, (byte) 0x04   // DESCRIPTOR ID CODE
    }), 0);

    //Accessor keys for field mapped fields:
    private static final AmqpSymbol.AmqpSymbolBuffer TRUNCATE_KEY = new AmqpSymbol.AmqpSymbolBean("truncate").getBuffer(AmqpMarshaller.SINGLETON);
    private static final AmqpSymbol.AmqpSymbolBuffer DELIVERY_FAILED_KEY = new AmqpSymbol.AmqpSymbolBean("delivery-failed").getBuffer(AmqpMarshaller.SINGLETON);
    private static final AmqpSymbol.AmqpSymbolBuffer DELIVER_ELSEWHERE_KEY = new AmqpSymbol.AmqpSymbolBean("deliver-elsewhere").getBuffer(AmqpMarshaller.SINGLETON);
    private static final AmqpSymbol.AmqpSymbolBuffer MESSAGE_ATTRS_KEY = new AmqpSymbol.AmqpSymbolBean("message-attrs").getBuffer(AmqpMarshaller.SINGLETON);
    private static final AmqpSymbol.AmqpSymbolBuffer DELIVERY_ATTRS_KEY = new AmqpSymbol.AmqpSymbolBean("delivery-attrs").getBuffer(AmqpMarshaller.SINGLETON);


    private static final MapDecoder DECODER = new MapDecoder() {
        public void decodeToMap(EncodedBuffer encodedKey, EncodedBuffer encodedValue, Map<AmqpType<?, ?>,AmqpType<?, ?>> map) throws AmqpEncodingError {
            AmqpSymbol key = AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(encodedKey));
            if (key == null) {
                throw new AmqpEncodingError("Null Key for " + SYMBOLIC_ID);
            }

            if (key.getValue().equals(TRUNCATE_KEY.getValue())){
                map.put(TRUNCATE_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(encodedValue)));
            }
            if (key.getValue().equals(DELIVERY_FAILED_KEY.getValue())){
                map.put(DELIVERY_FAILED_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(encodedValue)));
            }
            if (key.getValue().equals(DELIVER_ELSEWHERE_KEY.getValue())){
                map.put(DELIVER_ELSEWHERE_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(encodedValue)));
            }
            if (key.getValue().equals(MESSAGE_ATTRS_KEY.getValue())){
                map.put(MESSAGE_ATTRS_KEY, AmqpMessageAttributes.AmqpMessageAttributesBuffer.create(AmqpMapMarshaller.createEncoded(encodedValue)));
            }
            if (key.getValue().equals(DELIVERY_ATTRS_KEY.getValue())){
                map.put(DELIVERY_ATTRS_KEY, AmqpMessageAttributes.AmqpMessageAttributesBuffer.create(AmqpMapMarshaller.createEncoded(encodedValue)));
            }
            else {
                throw new UnexpectedTypeException("Invalid Key for " + SYMBOLIC_ID + " : " + key);
            }
        }
        public void unmarshalToMap(DataInput in, Map<AmqpType<?, ?>,AmqpType<?, ?>> map) throws AmqpEncodingError, IOException {
            AmqpSymbol key = AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(in));
            if (key == null) {
                throw new AmqpEncodingError("Null Key for " + SYMBOLIC_ID);
            }

            if (key.getValue().equals(TRUNCATE_KEY.getValue())){
                map.put(TRUNCATE_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(in)));
            }
            if (key.getValue().equals(DELIVERY_FAILED_KEY.getValue())){
                map.put(DELIVERY_FAILED_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(in)));
            }
            if (key.getValue().equals(DELIVER_ELSEWHERE_KEY.getValue())){
                map.put(DELIVER_ELSEWHERE_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(in)));
            }
            if (key.getValue().equals(MESSAGE_ATTRS_KEY.getValue())){
                map.put(MESSAGE_ATTRS_KEY, AmqpMessageAttributes.AmqpMessageAttributesBuffer.create(AmqpMapMarshaller.createEncoded(in)));
            }
            if (key.getValue().equals(DELIVERY_ATTRS_KEY.getValue())){
                map.put(DELIVERY_ATTRS_KEY, AmqpMessageAttributes.AmqpMessageAttributesBuffer.create(AmqpMapMarshaller.createEncoded(in)));
            }
            else {
                throw new UnexpectedTypeException("Invalid Key for " + SYMBOLIC_ID + " : " + key);
            }
        }
    };

    public static class AmqpReleasedEncoded extends DescribedEncoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> {

        public AmqpReleasedEncoded(DescribedBuffer buffer) {
            super(buffer);
        }

        public AmqpReleasedEncoded(AmqpReleased value) {
            super(AmqpMapMarshaller.encode(value));
        }

        protected final String getSymbolicId() {
            return SYMBOLIC_ID;
        }

        protected final long getNumericId() {
            return NUMERIC_ID;
        }

        protected final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeDescribed(EncodedBuffer encoded) throws AmqpEncodingError {
            return AmqpMapMarshaller.createEncoded(encoded, DECODER);
        }

        protected final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalDescribed(DataInput in) throws IOException {
            return AmqpMapMarshaller.createEncoded(in, DECODER);
        }

        protected final EncodedBuffer getDescriptor() {
            return DESCRIPTOR;
        }
    }

    public static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpReleased value) throws AmqpEncodingError {
        return new AmqpReleasedEncoded(value);
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(Buffer source, int offset) throws AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(source, offset));
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(DataInput in) throws IOException, AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(in.readByte(), in));
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {
        byte fc = buffer.getEncodingFormatCode();
        if (fc == Encoder.NULL_FORMAT_CODE) {
            return NULL_ENCODED;
        }

        DescribedBuffer db = buffer.asDescribed();
        AmqpType<?, ?> descriptor = AmqpMarshaller.SINGLETON.decodeType(db.getDescriptorBuffer());
        if(!(descriptor instanceof AmqpUlong && ((AmqpUlong)descriptor).getValue().longValue() == NUMERIC_ID ||
               descriptor instanceof AmqpSymbol && ((AmqpSymbol)descriptor).getValue().equals(SYMBOLIC_ID))) {
            throw new UnexpectedTypeException("descriptor mismatch: " + descriptor);
        }
        return new AmqpReleasedEncoded(db);
    }

    public final AmqpReleased.AmqpReleasedBuffer decodeDescribedType(AmqpType<?, ?> descriptor, DescribedBuffer encoded) throws AmqpEncodingError {
        return AmqpReleased.AmqpReleasedBuffer.create(new AmqpReleasedEncoded(encoded));
    }
}
