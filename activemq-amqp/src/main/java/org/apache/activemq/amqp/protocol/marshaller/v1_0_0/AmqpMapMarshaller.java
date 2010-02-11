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
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpVersion;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.marshaller.Encoding;
import org.apache.activemq.amqp.protocol.marshaller.UnexpectedTypeException;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder.*;
import org.apache.activemq.amqp.protocol.types.AmqpMap;
import org.apache.activemq.amqp.protocol.types.AmqpType;
import org.apache.activemq.util.buffer.Buffer;

public class AmqpMapMarshaller {

    private static final Encoder ENCODER = Encoder.SINGLETON;
    private static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> NULL_ENCODED = new Encoder.NullEncoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>>();

    public static final byte MAP8_FORMAT_CODE = (byte) 0xc1;
    public static final byte MAP32_FORMAT_CODE = (byte) 0xd1;

    public static enum MAP_ENCODING implements Encoding{
        MAP8 (MAP8_FORMAT_CODE), // up to 2^8 - 1 octets of encoded map data
        MAP32 (MAP32_FORMAT_CODE); // up to 2^32 - 1 octets of encoded map data

        public final byte FORMAT_CODE;
        public final FormatSubCategory CATEGORY;

        MAP_ENCODING(byte formatCode) {
            this.FORMAT_CODE = formatCode;
            this.CATEGORY = FormatSubCategory.getCategory(formatCode);
        }

        public final byte getEncodingFormatCode() {
            return FORMAT_CODE;
        }

        public final AmqpVersion getEncodingVersion() {
            return AmqpMarshaller.VERSION;
        }

        public static MAP_ENCODING getEncoding(byte formatCode) throws UnexpectedTypeException {
            switch(formatCode) {
            case MAP8_FORMAT_CODE: {
                return MAP8;
            }
            case MAP32_FORMAT_CODE: {
                return MAP32;
            }
            default: {
                throw new UnexpectedTypeException("Unexpected format code for Map: " + formatCode);
            }
            }
        }

        static final AmqpMapEncoded createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {
            switch(buffer.getEncodingFormatCode()) {
            case MAP8_FORMAT_CODE: {
                return new AmqpMapMap8Encoded(buffer);
            }
            case MAP32_FORMAT_CODE: {
                return new AmqpMapMap32Encoded(buffer);
            }
            default: {
                throw new UnexpectedTypeException("Unexpected format code for Map: " + buffer.getEncodingFormatCode());
            }
            }
        }
        static final AmqpMapEncoded createEncoded(byte formatCode, HashMap<AmqpType<?,?>, AmqpType<?,?>> value) throws AmqpEncodingError {
            switch(formatCode) {
            case MAP8_FORMAT_CODE: {
                return new AmqpMapMap8Encoded(value);
            }
            case MAP32_FORMAT_CODE: {
                return new AmqpMapMap32Encoded(value);
            }
            default: {
                throw new UnexpectedTypeException("Unexpected format code for Map: " + formatCode);
            }
            }
        }
    }
    public static abstract class AmqpMapEncoded extends AbstractEncoded <HashMap<AmqpType<?,?>, AmqpType<?,?>>> {
        MapDecoder decoder = Encoder.DEFAULT_MAP_DECODER;

        public AmqpMapEncoded(EncodedBuffer encoded) {
            super(encoded);
        }

        public AmqpMapEncoded(byte formatCode, HashMap<AmqpType<?,?>, AmqpType<?,?>> value) throws AmqpEncodingError {
            super(formatCode, value);
        }

        final void setDecoder(MapDecoder decoder) {
            this.decoder = decoder;
        }
    }

    /**
     * up to 2^8 - 1 octets of encoded map data
     */
    private static class AmqpMapMap8Encoded extends AmqpMapEncoded {

        private final MAP_ENCODING encoding = MAP_ENCODING.MAP8;
        public AmqpMapMap8Encoded(EncodedBuffer encoded) {
            super(encoded);
        }

        public AmqpMapMap8Encoded(HashMap<AmqpType<?,?>, AmqpType<?,?>> value) throws AmqpEncodingError {
            super(MAP_ENCODING.MAP8.FORMAT_CODE, value);
        }

        protected final int computeDataSize() throws AmqpEncodingError {
            return ENCODER.getEncodedSizeOfMap(value, encoding);
        }

        protected final int computeDataCount() throws AmqpEncodingError {
            return ENCODER.getEncodedCountOfMap(value, encoding);
        }

        public final void encode(HashMap<AmqpType<?,?>, AmqpType<?,?>> value, Buffer encoded, int offset) throws AmqpEncodingError {
            ENCODER.encodeMapMap8(value, encoded, offset);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> decode(EncodedBuffer encoded) throws AmqpEncodingError {
            return ENCODER.decodeMapMap8(encoded.getBuffer(), encoded.getDataOffset(), encoded.getDataCount(), encoded.getDataSize(), decoder);
        }

        public final void marshalData(DataOutput out) throws IOException {
            ENCODER.writeMapMap8(value, out);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> unmarshalData(DataInput in) throws IOException {
            return ENCODER.readMapMap8(getDataCount(), getDataSize(), in, decoder);
        }
    }

    /**
     * up to 2^32 - 1 octets of encoded map data
     */
    private static class AmqpMapMap32Encoded extends AmqpMapEncoded {

        private final MAP_ENCODING encoding = MAP_ENCODING.MAP32;
        public AmqpMapMap32Encoded(EncodedBuffer encoded) {
            super(encoded);
        }

        public AmqpMapMap32Encoded(HashMap<AmqpType<?,?>, AmqpType<?,?>> value) throws AmqpEncodingError {
            super(MAP_ENCODING.MAP32.FORMAT_CODE, value);
        }

        protected final int computeDataSize() throws AmqpEncodingError {
            return ENCODER.getEncodedSizeOfMap(value, encoding);
        }

        protected final int computeDataCount() throws AmqpEncodingError {
            return ENCODER.getEncodedCountOfMap(value, encoding);
        }

        public final void encode(HashMap<AmqpType<?,?>, AmqpType<?,?>> value, Buffer encoded, int offset) throws AmqpEncodingError {
            ENCODER.encodeMapMap32(value, encoded, offset);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> decode(EncodedBuffer encoded) throws AmqpEncodingError {
            return ENCODER.decodeMapMap32(encoded.getBuffer(), encoded.getDataOffset(), encoded.getDataCount(), encoded.getDataSize(), decoder);
        }

        public final void marshalData(DataOutput out) throws IOException {
            ENCODER.writeMapMap32(value, out);
        }

        public final HashMap<AmqpType<?,?>, AmqpType<?,?>> unmarshalData(DataInput in) throws IOException {
            return ENCODER.readMapMap32(getDataCount(), getDataSize(), in, decoder);
        }
    }


    private static final MAP_ENCODING chooseEncoding(AmqpMap val) throws AmqpEncodingError {
        return Encoder.chooseMapEncoding(val.getValue());
    }

    private static final MAP_ENCODING chooseEncoding(HashMap<AmqpType<?,?>, AmqpType<?,?>> val) throws AmqpEncodingError {
        return Encoder.chooseMapEncoding(val);
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpMap data) throws AmqpEncodingError {
        if(data == null) {
            return NULL_ENCODED;
        }
        return MAP_ENCODING.createEncoded(chooseEncoding(data).FORMAT_CODE, data.getValue());
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(Buffer source, int offset) throws AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(source, offset));
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(HashMap<AmqpType<?,?>, AmqpType<?,?>> val) throws AmqpEncodingError {
        return MAP_ENCODING.createEncoded(chooseEncoding(val).FORMAT_CODE, val);
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(DataInput in) throws IOException, AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(in.readByte(), in));
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {
        if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {
            return NULL_ENCODED;
        }
        return MAP_ENCODING.createEncoded(buffer);
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(DataInput in, MapDecoder decoder) throws IOException, AmqpEncodingError {
        return createEncoded(FormatCategory.createBuffer(in.readByte(), in), decoder);
    }

    static final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> createEncoded(EncodedBuffer buffer, MapDecoder decoder) throws AmqpEncodingError {
        if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {
            return NULL_ENCODED;
        }
        AmqpMapEncoded rc = MAP_ENCODING.createEncoded(buffer);
        rc.setDecoder(decoder);
        return rc;
    }
}
