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
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.marshaller.UnexpectedTypeException;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder.*;
import org.apache.activemq.amqp.protocol.types.AmqpAddress;
import org.apache.activemq.amqp.protocol.types.AmqpBoolean;
import org.apache.activemq.amqp.protocol.types.AmqpSymbol;
import org.apache.activemq.amqp.protocol.types.AmqpTarget;
import org.apache.activemq.amqp.protocol.types.AmqpType;
import org.apache.activemq.amqp.protocol.types.AmqpUint;
import org.apache.activemq.amqp.protocol.types.AmqpUlong;
import org.apache.activemq.amqp.protocol.types.IAmqpMap;
import org.apache.activemq.util.buffer.Buffer;

public class AmqpTargetMarshaller implements DescribedTypeMarshaller<AmqpTarget>{

    static final AmqpTargetMarshaller SINGLETON = new AmqpTargetMarshaller();
    private static final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> NULL_ENCODED = new Encoder.NullEncoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>>();

    public static final String SYMBOLIC_ID = "amqp:target:map";
    //Format code: 0x00000001:0x00009702:
    public static final long CATEGORY = 1;
    public static final long DESCRIPTOR_ID = 38658;
    public static final long NUMERIC_ID = CATEGORY << 32 | DESCRIPTOR_ID; //(4295005954L)
    //Hard coded descriptor:
    public static final EncodedBuffer DESCRIPTOR = FormatCategory.createBuffer(new Buffer(new byte [] {
        (byte) 0x80,                                         // ulong descriptor encoding)
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,  // CATEGORY CODE
        (byte) 0x00, (byte) 0x00, (byte) 0x97, (byte) 0x02   // DESCRIPTOR ID CODE
    }), 0);

    private static final MapDecoder<AmqpSymbol, AmqpType<?, ?>> DECODER = new MapDecoder<AmqpSymbol, AmqpType<?, ?>>() {

        public IAmqpMap<AmqpSymbol, AmqpType<?, ?>> createMap(int entryCount) {
            return new IAmqpMap.AmqpWrapperMap<AmqpSymbol, AmqpType<?,?>>(new HashMap<AmqpSymbol, AmqpType<?,?>>());

        }

        public void decodeToMap(EncodedBuffer encodedKey, EncodedBuffer encodedValue, IAmqpMap<AmqpSymbol,AmqpType<?, ?>> map) throws AmqpEncodingError {
            AmqpSymbol key = AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(encodedKey));
            if (key == null) {
                throw new AmqpEncodingError("Null Key for " + SYMBOLIC_ID);
            }

            if (key.equals(AmqpTarget.ADDRESS_KEY)){
                map.put(AmqpTarget.ADDRESS_KEY, AmqpAddress.AmqpAddressBuffer.create(AmqpBinaryMarshaller.createEncoded(encodedValue)));
            }
            if (key.equals(AmqpTarget.CREATE_KEY)){
                map.put(AmqpTarget.CREATE_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(encodedValue)));
            }
            if (key.equals(AmqpTarget.TIMEOUT_KEY)){
                map.put(AmqpTarget.TIMEOUT_KEY, AmqpUint.AmqpUintBuffer.create(AmqpUintMarshaller.createEncoded(encodedValue)));
            }
            else {
                throw new UnexpectedTypeException("Invalid Key for " + SYMBOLIC_ID + " : " + key);
            }
        }

        public void unmarshalToMap(DataInput in, IAmqpMap<AmqpSymbol,AmqpType<?, ?>> map) throws IOException, AmqpEncodingError {
            AmqpSymbol key = AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(in));
            if (key == null) {
                throw new AmqpEncodingError("Null Key for " + SYMBOLIC_ID);
            }

            if (key.equals(AmqpTarget.ADDRESS_KEY)){
                map.put(AmqpTarget.ADDRESS_KEY, AmqpAddress.AmqpAddressBuffer.create(AmqpBinaryMarshaller.createEncoded(in)));
            }
            if (key.equals(AmqpTarget.CREATE_KEY)){
                map.put(AmqpTarget.CREATE_KEY, AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(in)));
            }
            if (key.equals(AmqpTarget.TIMEOUT_KEY)){
                map.put(AmqpTarget.TIMEOUT_KEY, AmqpUint.AmqpUintBuffer.create(AmqpUintMarshaller.createEncoded(in)));
            }
            else {
                throw new UnexpectedTypeException("Invalid Key for " + SYMBOLIC_ID + " : " + key);
            }
        }
    };

    public static class AmqpTargetEncoded extends DescribedEncoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> {

        public AmqpTargetEncoded(DescribedBuffer buffer) {
            super(buffer);
        }

        public AmqpTargetEncoded(AmqpTarget value) {
            super(AmqpMapMarshaller.encode(value));
        }

        protected final String getSymbolicId() {
            return SYMBOLIC_ID;
        }

        protected final long getNumericId() {
            return NUMERIC_ID;
        }

        protected final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> decodeDescribed(EncodedBuffer encoded) throws AmqpEncodingError {
            return AmqpMapMarshaller.createEncoded(encoded, DECODER);
        }

        protected final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> unmarshalDescribed(DataInput in) throws IOException {
            return AmqpMapMarshaller.createEncoded(in, DECODER);
        }

        protected final EncodedBuffer getDescriptor() {
            return DESCRIPTOR;
        }
    }

    public static final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> encode(AmqpTarget value) throws AmqpEncodingError {
        return new AmqpTargetEncoded(value);
    }

    static final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> createEncoded(Buffer source, int offset) throws AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(source, offset));
    }

    static final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> createEncoded(DataInput in) throws IOException, AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(in.readByte(), in));
    }

    static final Encoded<IAmqpMap<AmqpType<?, ?>, AmqpType<?, ?>>> createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {
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
        return new AmqpTargetEncoded(db);
    }

    public final AmqpTarget.AmqpTargetBuffer decodeDescribedType(AmqpType<?, ?> descriptor, DescribedBuffer encoded) throws AmqpEncodingError {
        return AmqpTarget.AmqpTargetBuffer.create(new AmqpTargetEncoded(encoded));
    }
}
