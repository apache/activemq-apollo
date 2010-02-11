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
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpVersion;
import org.apache.activemq.amqp.protocol.marshaller.Encoded;
import org.apache.activemq.amqp.protocol.marshaller.v1_0_0.Encoder.*;
import org.apache.activemq.amqp.protocol.types.AmqpAttach;
import org.apache.activemq.amqp.protocol.types.AmqpBar;
import org.apache.activemq.amqp.protocol.types.AmqpBinary;
import org.apache.activemq.amqp.protocol.types.AmqpBoolean;
import org.apache.activemq.amqp.protocol.types.AmqpByte;
import org.apache.activemq.amqp.protocol.types.AmqpChar;
import org.apache.activemq.amqp.protocol.types.AmqpClose;
import org.apache.activemq.amqp.protocol.types.AmqpCompleted;
import org.apache.activemq.amqp.protocol.types.AmqpConnectionError;
import org.apache.activemq.amqp.protocol.types.AmqpDetach;
import org.apache.activemq.amqp.protocol.types.AmqpDisposition;
import org.apache.activemq.amqp.protocol.types.AmqpDouble;
import org.apache.activemq.amqp.protocol.types.AmqpDrain;
import org.apache.activemq.amqp.protocol.types.AmqpEnlist;
import org.apache.activemq.amqp.protocol.types.AmqpFilter;
import org.apache.activemq.amqp.protocol.types.AmqpFloat;
import org.apache.activemq.amqp.protocol.types.AmqpFlow;
import org.apache.activemq.amqp.protocol.types.AmqpFooter;
import org.apache.activemq.amqp.protocol.types.AmqpFragment;
import org.apache.activemq.amqp.protocol.types.AmqpHeader;
import org.apache.activemq.amqp.protocol.types.AmqpInt;
import org.apache.activemq.amqp.protocol.types.AmqpLink;
import org.apache.activemq.amqp.protocol.types.AmqpLinkError;
import org.apache.activemq.amqp.protocol.types.AmqpList;
import org.apache.activemq.amqp.protocol.types.AmqpLong;
import org.apache.activemq.amqp.protocol.types.AmqpMap;
import org.apache.activemq.amqp.protocol.types.AmqpNoop;
import org.apache.activemq.amqp.protocol.types.AmqpNull;
import org.apache.activemq.amqp.protocol.types.AmqpOpen;
import org.apache.activemq.amqp.protocol.types.AmqpProperties;
import org.apache.activemq.amqp.protocol.types.AmqpRejected;
import org.apache.activemq.amqp.protocol.types.AmqpReleased;
import org.apache.activemq.amqp.protocol.types.AmqpRelink;
import org.apache.activemq.amqp.protocol.types.AmqpSaslChallenge;
import org.apache.activemq.amqp.protocol.types.AmqpSaslInit;
import org.apache.activemq.amqp.protocol.types.AmqpSaslMechanisms;
import org.apache.activemq.amqp.protocol.types.AmqpSaslOutcome;
import org.apache.activemq.amqp.protocol.types.AmqpSaslResponse;
import org.apache.activemq.amqp.protocol.types.AmqpSessionError;
import org.apache.activemq.amqp.protocol.types.AmqpShort;
import org.apache.activemq.amqp.protocol.types.AmqpSource;
import org.apache.activemq.amqp.protocol.types.AmqpString;
import org.apache.activemq.amqp.protocol.types.AmqpSymbol;
import org.apache.activemq.amqp.protocol.types.AmqpTarget;
import org.apache.activemq.amqp.protocol.types.AmqpTimestamp;
import org.apache.activemq.amqp.protocol.types.AmqpTransfer;
import org.apache.activemq.amqp.protocol.types.AmqpTxn;
import org.apache.activemq.amqp.protocol.types.AmqpType;
import org.apache.activemq.amqp.protocol.types.AmqpUbyte;
import org.apache.activemq.amqp.protocol.types.AmqpUint;
import org.apache.activemq.amqp.protocol.types.AmqpUlong;
import org.apache.activemq.amqp.protocol.types.AmqpUnlink;
import org.apache.activemq.amqp.protocol.types.AmqpUshort;
import org.apache.activemq.amqp.protocol.types.AmqpUuid;
import org.apache.activemq.amqp.protocol.types.AmqpXid;
import org.apache.activemq.amqp.protocol.types.IAmqpList;
import org.apache.activemq.util.buffer.Buffer;

public class AmqpMarshaller implements org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller {

    static final AmqpMarshaller SINGLETON = new AmqpMarshaller();

    public static final AmqpVersion VERSION = new AmqpVersion((short)1, (short)0, (short)0);

    public static final AmqpMarshaller getMarshaller() {
        return SINGLETON;
    }

    private static final HashMap<Long, DescribedTypeMarshaller<?>> DESCRIBED_NUMERIC_TYPES = new HashMap<Long, DescribedTypeMarshaller<?>>();
    private static final HashMap<String, DescribedTypeMarshaller<?>> DESCRIBED_SYMBOLIC_TYPES = new HashMap<String, DescribedTypeMarshaller<?>>();
    static {
        DESCRIBED_NUMERIC_TYPES.put(AmqpSessionErrorMarshaller.NUMERIC_ID, AmqpSessionErrorMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSessionErrorMarshaller.SYMBOLIC_ID, AmqpSessionErrorMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpUnlinkMarshaller.NUMERIC_ID, AmqpUnlinkMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpUnlinkMarshaller.SYMBOLIC_ID, AmqpUnlinkMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpFlowMarshaller.NUMERIC_ID, AmqpFlowMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpFlowMarshaller.SYMBOLIC_ID, AmqpFlowMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpRelinkMarshaller.NUMERIC_ID, AmqpRelinkMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpRelinkMarshaller.SYMBOLIC_ID, AmqpRelinkMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpCloseMarshaller.NUMERIC_ID, AmqpCloseMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpCloseMarshaller.SYMBOLIC_ID, AmqpCloseMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpOpenMarshaller.NUMERIC_ID, AmqpOpenMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpOpenMarshaller.SYMBOLIC_ID, AmqpOpenMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpRejectedMarshaller.NUMERIC_ID, AmqpRejectedMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpRejectedMarshaller.SYMBOLIC_ID, AmqpRejectedMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpSaslOutcomeMarshaller.NUMERIC_ID, AmqpSaslOutcomeMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSaslOutcomeMarshaller.SYMBOLIC_ID, AmqpSaslOutcomeMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpTransferMarshaller.NUMERIC_ID, AmqpTransferMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpTransferMarshaller.SYMBOLIC_ID, AmqpTransferMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpDetachMarshaller.NUMERIC_ID, AmqpDetachMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpDetachMarshaller.SYMBOLIC_ID, AmqpDetachMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpSaslInitMarshaller.NUMERIC_ID, AmqpSaslInitMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSaslInitMarshaller.SYMBOLIC_ID, AmqpSaslInitMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpEnlistMarshaller.NUMERIC_ID, AmqpEnlistMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpEnlistMarshaller.SYMBOLIC_ID, AmqpEnlistMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpPropertiesMarshaller.NUMERIC_ID, AmqpPropertiesMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpPropertiesMarshaller.SYMBOLIC_ID, AmqpPropertiesMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpDrainMarshaller.NUMERIC_ID, AmqpDrainMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpDrainMarshaller.SYMBOLIC_ID, AmqpDrainMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpHeaderMarshaller.NUMERIC_ID, AmqpHeaderMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpHeaderMarshaller.SYMBOLIC_ID, AmqpHeaderMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpSaslMechanismsMarshaller.NUMERIC_ID, AmqpSaslMechanismsMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSaslMechanismsMarshaller.SYMBOLIC_ID, AmqpSaslMechanismsMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpBarMarshaller.NUMERIC_ID, AmqpBarMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpBarMarshaller.SYMBOLIC_ID, AmqpBarMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpFilterMarshaller.NUMERIC_ID, AmqpFilterMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpFilterMarshaller.SYMBOLIC_ID, AmqpFilterMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpNoopMarshaller.NUMERIC_ID, AmqpNoopMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpNoopMarshaller.SYMBOLIC_ID, AmqpNoopMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpLinkMarshaller.NUMERIC_ID, AmqpLinkMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpLinkMarshaller.SYMBOLIC_ID, AmqpLinkMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpReleasedMarshaller.NUMERIC_ID, AmqpReleasedMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpReleasedMarshaller.SYMBOLIC_ID, AmqpReleasedMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpTxnMarshaller.NUMERIC_ID, AmqpTxnMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpTxnMarshaller.SYMBOLIC_ID, AmqpTxnMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpLinkErrorMarshaller.NUMERIC_ID, AmqpLinkErrorMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpLinkErrorMarshaller.SYMBOLIC_ID, AmqpLinkErrorMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpConnectionErrorMarshaller.NUMERIC_ID, AmqpConnectionErrorMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpConnectionErrorMarshaller.SYMBOLIC_ID, AmqpConnectionErrorMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpSaslResponseMarshaller.NUMERIC_ID, AmqpSaslResponseMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSaslResponseMarshaller.SYMBOLIC_ID, AmqpSaslResponseMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpFragmentMarshaller.NUMERIC_ID, AmqpFragmentMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpFragmentMarshaller.SYMBOLIC_ID, AmqpFragmentMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpDispositionMarshaller.NUMERIC_ID, AmqpDispositionMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpDispositionMarshaller.SYMBOLIC_ID, AmqpDispositionMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpCompletedMarshaller.NUMERIC_ID, AmqpCompletedMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpCompletedMarshaller.SYMBOLIC_ID, AmqpCompletedMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpXidMarshaller.NUMERIC_ID, AmqpXidMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpXidMarshaller.SYMBOLIC_ID, AmqpXidMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpFooterMarshaller.NUMERIC_ID, AmqpFooterMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpFooterMarshaller.SYMBOLIC_ID, AmqpFooterMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpSourceMarshaller.NUMERIC_ID, AmqpSourceMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSourceMarshaller.SYMBOLIC_ID, AmqpSourceMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpSaslChallengeMarshaller.NUMERIC_ID, AmqpSaslChallengeMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpSaslChallengeMarshaller.SYMBOLIC_ID, AmqpSaslChallengeMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpAttachMarshaller.NUMERIC_ID, AmqpAttachMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpAttachMarshaller.SYMBOLIC_ID, AmqpAttachMarshaller.SINGLETON);
        DESCRIBED_NUMERIC_TYPES.put(AmqpTargetMarshaller.NUMERIC_ID, AmqpTargetMarshaller.SINGLETON);
        DESCRIBED_SYMBOLIC_TYPES.put(AmqpTargetMarshaller.SYMBOLIC_ID, AmqpTargetMarshaller.SINGLETON);
    }

    /**
     * @return the protocol version of the marshaller
     */
    public final AmqpVersion getVersion() {
        return VERSION;
    }

    public final AmqpType<?, ?> unmarshalType(DataInput in) throws IOException, AmqpEncodingError {
        return Encoder.unmarshalType(in);
    }

    public final AmqpType<?, ?> decodeType(Buffer source) throws AmqpEncodingError {
        return Encoder.decode(source);
    }

    final AmqpType<?, ?> decodeType(EncodedBuffer encoded) throws AmqpEncodingError {
        if(encoded.isDescribed()) {
        return decodeType(encoded.asDescribed());
        }

        switch(encoded.getEncodingFormatCode()) {
        //AmqpChar Encoded: 
        case AmqpCharMarshaller.FORMAT_CODE: 
        {
            return AmqpChar.AmqpCharBuffer.create(AmqpCharMarshaller.createEncoded(encoded));
        }
        //AmqpLong Encoded: 
        case AmqpLongMarshaller.FORMAT_CODE: 
        {
            return AmqpLong.AmqpLongBuffer.create(AmqpLongMarshaller.createEncoded(encoded));
        }
        //AmqpFloat Encoded: 
        case AmqpFloatMarshaller.FORMAT_CODE: 
        {
            return AmqpFloat.AmqpFloatBuffer.create(AmqpFloatMarshaller.createEncoded(encoded));
        }
        //AmqpByte Encoded: 
        case AmqpByteMarshaller.FORMAT_CODE: 
        {
            return AmqpByte.AmqpByteBuffer.create(AmqpByteMarshaller.createEncoded(encoded));
        }
        //AmqpBoolean Encoded: 
        case (byte) 0x41:
        case (byte) 0x42:
        {
            return AmqpBoolean.AmqpBooleanBuffer.create(AmqpBooleanMarshaller.createEncoded(encoded));
        }
        //AmqpUlong Encoded: 
        case AmqpUlongMarshaller.FORMAT_CODE: 
        {
            return AmqpUlong.AmqpUlongBuffer.create(AmqpUlongMarshaller.createEncoded(encoded));
        }
        //AmqpSymbol Encoded: 
        case (byte) 0xa3:
        case (byte) 0xb3:
        {
            return AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(encoded));
        }
        //AmqpInt Encoded: 
        case AmqpIntMarshaller.FORMAT_CODE: 
        {
            return AmqpInt.AmqpIntBuffer.create(AmqpIntMarshaller.createEncoded(encoded));
        }
        //AmqpDouble Encoded: 
        case AmqpDoubleMarshaller.FORMAT_CODE: 
        {
            return AmqpDouble.AmqpDoubleBuffer.create(AmqpDoubleMarshaller.createEncoded(encoded));
        }
        //AmqpUuid Encoded: 
        case AmqpUuidMarshaller.FORMAT_CODE: 
        {
            return AmqpUuid.AmqpUuidBuffer.create(AmqpUuidMarshaller.createEncoded(encoded));
        }
        //AmqpBinary Encoded: 
        case (byte) 0xa0:
        case (byte) 0xb0:
        {
            return AmqpBinary.AmqpBinaryBuffer.create(AmqpBinaryMarshaller.createEncoded(encoded));
        }
        //AmqpTimestamp Encoded: 
        case AmqpTimestampMarshaller.FORMAT_CODE: 
        {
            return AmqpTimestamp.AmqpTimestampBuffer.create(AmqpTimestampMarshaller.createEncoded(encoded));
        }
        //AmqpShort Encoded: 
        case AmqpShortMarshaller.FORMAT_CODE: 
        {
            return AmqpShort.AmqpShortBuffer.create(AmqpShortMarshaller.createEncoded(encoded));
        }
        //AmqpMap Encoded: 
        case (byte) 0xc1:
        case (byte) 0xd1:
        {
            return AmqpMap.AmqpMapBuffer.create(AmqpMapMarshaller.createEncoded(encoded));
        }
        //AmqpNull Encoded: 
        case AmqpNullMarshaller.FORMAT_CODE: 
        {
            return AmqpNull.AmqpNullBuffer.create(AmqpNullMarshaller.createEncoded(encoded));
        }
        //AmqpList Encoded: 
        case (byte) 0xc0:
        case (byte) 0xd0:
        case (byte) 0xe0:
        case (byte) 0xf0:
        {
            return AmqpList.AmqpListBuffer.create(AmqpListMarshaller.createEncoded(encoded));
        }
        //AmqpUshort Encoded: 
        case AmqpUshortMarshaller.FORMAT_CODE: 
        {
            return AmqpUshort.AmqpUshortBuffer.create(AmqpUshortMarshaller.createEncoded(encoded));
        }
        //AmqpString Encoded: 
        case (byte) 0xa1:
        case (byte) 0xa2:
        case (byte) 0xb1:
        case (byte) 0xb2:
        {
            return AmqpString.AmqpStringBuffer.create(AmqpStringMarshaller.createEncoded(encoded));
        }
        //AmqpUbyte Encoded: 
        case AmqpUbyteMarshaller.FORMAT_CODE: 
        {
            return AmqpUbyte.AmqpUbyteBuffer.create(AmqpUbyteMarshaller.createEncoded(encoded));
        }
        //AmqpUint Encoded: 
        case AmqpUintMarshaller.FORMAT_CODE: 
        {
            return AmqpUint.AmqpUintBuffer.create(AmqpUintMarshaller.createEncoded(encoded));
        }
        default: {
            //TODO: Create an unknown or any type
            throw new AmqpEncodingError("Unrecognized format code:" + encoded.getEncodingFormatCode());
        }
        }
    }

    final AmqpType<?, ?> decodeType(DescribedBuffer buffer) throws AmqpEncodingError {
        AmqpType<?, ?> descriptor = decodeType(buffer.getDescriptorBuffer());
        //TODO might want to revisit whether or not the cast is needed here:
        DescribedTypeMarshaller<?> dtm = null;
        if(descriptor instanceof AmqpUlong) {
            dtm = DESCRIBED_NUMERIC_TYPES.get(((AmqpUlong)descriptor).getValue().longValue());
        }
        else if(descriptor instanceof AmqpSymbol) {
            dtm = DESCRIBED_SYMBOLIC_TYPES.get(((AmqpSymbol)descriptor).getValue());
        }

        if(dtm != null) {
            return dtm.decodeDescribedType(descriptor, buffer);
        }

        //TODO spec actuall indicates that we should be able to pass along unknown types. so we should just create
        //an placeholder type
        throw new AmqpEncodingError("Unrecognized described type:" + descriptor);
    }
    public final Encoded<IAmqpList> encode(AmqpSessionError data) throws AmqpEncodingError {
        return AmqpSessionErrorMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpSessionError(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSessionErrorMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpSessionError(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSessionErrorMarshaller.createEncoded(in);
    }

    public final Encoded<Integer> encode(AmqpChar data) throws AmqpEncodingError {
        return AmqpCharMarshaller.encode(data);
    }

    public Encoded<Integer> decodeAmqpChar(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpCharMarshaller.createEncoded(source, offset);
    }

    public Encoded<Integer> unmarshalAmqpChar(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpCharMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpUnlink data) throws AmqpEncodingError {
        return AmqpUnlinkMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpUnlink(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpUnlinkMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpUnlink(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpUnlinkMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpFlow data) throws AmqpEncodingError {
        return AmqpFlowMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpFlow(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpFlowMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpFlow(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpFlowMarshaller.createEncoded(in);
    }

    public final Encoded<Long> encode(AmqpLong data) throws AmqpEncodingError {
        return AmqpLongMarshaller.encode(data);
    }

    public Encoded<Long> decodeAmqpLong(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpLongMarshaller.createEncoded(source, offset);
    }

    public Encoded<Long> unmarshalAmqpLong(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpLongMarshaller.createEncoded(in);
    }

    public final Encoded<Float> encode(AmqpFloat data) throws AmqpEncodingError {
        return AmqpFloatMarshaller.encode(data);
    }

    public Encoded<Float> decodeAmqpFloat(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpFloatMarshaller.createEncoded(source, offset);
    }

    public Encoded<Float> unmarshalAmqpFloat(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpFloatMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpRelink data) throws AmqpEncodingError {
        return AmqpRelinkMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpRelink(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpRelinkMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpRelink(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpRelinkMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpClose data) throws AmqpEncodingError {
        return AmqpCloseMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpClose(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpCloseMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpClose(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpCloseMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpOpen data) throws AmqpEncodingError {
        return AmqpOpenMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpOpen(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpOpenMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpOpen(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpOpenMarshaller.createEncoded(in);
    }

    public final Encoded<Byte> encode(AmqpByte data) throws AmqpEncodingError {
        return AmqpByteMarshaller.encode(data);
    }

    public Encoded<Byte> decodeAmqpByte(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpByteMarshaller.createEncoded(source, offset);
    }

    public Encoded<Byte> unmarshalAmqpByte(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpByteMarshaller.createEncoded(in);
    }

    public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpRejected data) throws AmqpEncodingError {
        return AmqpRejectedMarshaller.encode(data);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpRejected(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpRejectedMarshaller.createEncoded(source, offset);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpRejected(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpRejectedMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpSaslOutcome data) throws AmqpEncodingError {
        return AmqpSaslOutcomeMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpSaslOutcome(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSaslOutcomeMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpSaslOutcome(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSaslOutcomeMarshaller.createEncoded(in);
    }

    public final Encoded<Boolean> encode(AmqpBoolean data) throws AmqpEncodingError {
        return AmqpBooleanMarshaller.encode(data);
    }

    public Encoded<Boolean> decodeAmqpBoolean(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpBooleanMarshaller.createEncoded(source, offset);
    }

    public Encoded<Boolean> unmarshalAmqpBoolean(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpBooleanMarshaller.createEncoded(in);
    }

    public final Encoded<BigInteger> encode(AmqpUlong data) throws AmqpEncodingError {
        return AmqpUlongMarshaller.encode(data);
    }

    public Encoded<BigInteger> decodeAmqpUlong(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpUlongMarshaller.createEncoded(source, offset);
    }

    public Encoded<BigInteger> unmarshalAmqpUlong(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpUlongMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpTransfer data) throws AmqpEncodingError {
        return AmqpTransferMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpTransfer(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpTransferMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpTransfer(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpTransferMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpDetach data) throws AmqpEncodingError {
        return AmqpDetachMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpDetach(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpDetachMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpDetach(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpDetachMarshaller.createEncoded(in);
    }

    public final Encoded<String> encode(AmqpSymbol data) throws AmqpEncodingError {
        return AmqpSymbolMarshaller.encode(data);
    }

    public Encoded<String> decodeAmqpSymbol(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSymbolMarshaller.createEncoded(source, offset);
    }

    public Encoded<String> unmarshalAmqpSymbol(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSymbolMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpSaslInit data) throws AmqpEncodingError {
        return AmqpSaslInitMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpSaslInit(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSaslInitMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpSaslInit(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSaslInitMarshaller.createEncoded(in);
    }

    public final Encoded<Integer> encode(AmqpInt data) throws AmqpEncodingError {
        return AmqpIntMarshaller.encode(data);
    }

    public Encoded<Integer> decodeAmqpInt(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpIntMarshaller.createEncoded(source, offset);
    }

    public Encoded<Integer> unmarshalAmqpInt(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpIntMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpEnlist data) throws AmqpEncodingError {
        return AmqpEnlistMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpEnlist(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpEnlistMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpEnlist(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpEnlistMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpProperties data) throws AmqpEncodingError {
        return AmqpPropertiesMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpProperties(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpPropertiesMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpProperties(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpPropertiesMarshaller.createEncoded(in);
    }

    public final Encoded<Double> encode(AmqpDouble data) throws AmqpEncodingError {
        return AmqpDoubleMarshaller.encode(data);
    }

    public Encoded<Double> decodeAmqpDouble(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpDoubleMarshaller.createEncoded(source, offset);
    }

    public Encoded<Double> unmarshalAmqpDouble(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpDoubleMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpDrain data) throws AmqpEncodingError {
        return AmqpDrainMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpDrain(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpDrainMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpDrain(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpDrainMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpHeader data) throws AmqpEncodingError {
        return AmqpHeaderMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpHeader(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpHeaderMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpHeader(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpHeaderMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpSaslMechanisms data) throws AmqpEncodingError {
        return AmqpSaslMechanismsMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpSaslMechanisms(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSaslMechanismsMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpSaslMechanisms(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSaslMechanismsMarshaller.createEncoded(in);
    }

    public final Encoded<UUID> encode(AmqpUuid data) throws AmqpEncodingError {
        return AmqpUuidMarshaller.encode(data);
    }

    public Encoded<UUID> decodeAmqpUuid(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpUuidMarshaller.createEncoded(source, offset);
    }

    public Encoded<UUID> unmarshalAmqpUuid(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpUuidMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpBar data) throws AmqpEncodingError {
        return AmqpBarMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpBar(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpBarMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpBar(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpBarMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpFilter data) throws AmqpEncodingError {
        return AmqpFilterMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpFilter(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpFilterMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpFilter(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpFilterMarshaller.createEncoded(in);
    }

    public final Encoded<Buffer> encode(AmqpBinary data) throws AmqpEncodingError {
        return AmqpBinaryMarshaller.encode(data);
    }

    public Encoded<Buffer> decodeAmqpBinary(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpBinaryMarshaller.createEncoded(source, offset);
    }

    public Encoded<Buffer> unmarshalAmqpBinary(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpBinaryMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpNoop data) throws AmqpEncodingError {
        return AmqpNoopMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpNoop(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpNoopMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpNoop(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpNoopMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpLink data) throws AmqpEncodingError {
        return AmqpLinkMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpLink(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpLinkMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpLink(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpLinkMarshaller.createEncoded(in);
    }

    public final Encoded<Date> encode(AmqpTimestamp data) throws AmqpEncodingError {
        return AmqpTimestampMarshaller.encode(data);
    }

    public Encoded<Date> decodeAmqpTimestamp(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpTimestampMarshaller.createEncoded(source, offset);
    }

    public Encoded<Date> unmarshalAmqpTimestamp(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpTimestampMarshaller.createEncoded(in);
    }

    public final Encoded<Short> encode(AmqpShort data) throws AmqpEncodingError {
        return AmqpShortMarshaller.encode(data);
    }

    public Encoded<Short> decodeAmqpShort(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpShortMarshaller.createEncoded(source, offset);
    }

    public Encoded<Short> unmarshalAmqpShort(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpShortMarshaller.createEncoded(in);
    }

    public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpReleased data) throws AmqpEncodingError {
        return AmqpReleasedMarshaller.encode(data);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpReleased(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpReleasedMarshaller.createEncoded(source, offset);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpReleased(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpReleasedMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpTxn data) throws AmqpEncodingError {
        return AmqpTxnMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpTxn(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpTxnMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpTxn(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpTxnMarshaller.createEncoded(in);
    }

    public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpMap data) throws AmqpEncodingError {
        return AmqpMapMarshaller.encode(data);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpMap(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpMapMarshaller.createEncoded(source, offset);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpMap(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpMapMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpLinkError data) throws AmqpEncodingError {
        return AmqpLinkErrorMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpLinkError(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpLinkErrorMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpLinkError(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpLinkErrorMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpConnectionError data) throws AmqpEncodingError {
        return AmqpConnectionErrorMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpConnectionError(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpConnectionErrorMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpConnectionError(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpConnectionErrorMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpSaslResponse data) throws AmqpEncodingError {
        return AmqpSaslResponseMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpSaslResponse(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSaslResponseMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpSaslResponse(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSaslResponseMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpFragment data) throws AmqpEncodingError {
        return AmqpFragmentMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpFragment(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpFragmentMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpFragment(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpFragmentMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpDisposition data) throws AmqpEncodingError {
        return AmqpDispositionMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpDisposition(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpDispositionMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpDisposition(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpDispositionMarshaller.createEncoded(in);
    }

    public final Encoded<Object> encode(AmqpNull data) throws AmqpEncodingError {
        return AmqpNullMarshaller.encode(data);
    }

    public Encoded<Object> decodeAmqpNull(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpNullMarshaller.createEncoded(source, offset);
    }

    public Encoded<Object> unmarshalAmqpNull(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpNullMarshaller.createEncoded(in);
    }

    public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpCompleted data) throws AmqpEncodingError {
        return AmqpCompletedMarshaller.encode(data);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpCompleted(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpCompletedMarshaller.createEncoded(source, offset);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpCompleted(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpCompletedMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpXid data) throws AmqpEncodingError {
        return AmqpXidMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpXid(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpXidMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpXid(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpXidMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpList data) throws AmqpEncodingError {
        return AmqpListMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpList(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpListMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpList(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpListMarshaller.createEncoded(in);
    }

    public final Encoded<Integer> encode(AmqpUshort data) throws AmqpEncodingError {
        return AmqpUshortMarshaller.encode(data);
    }

    public Encoded<Integer> decodeAmqpUshort(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpUshortMarshaller.createEncoded(source, offset);
    }

    public Encoded<Integer> unmarshalAmqpUshort(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpUshortMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpFooter data) throws AmqpEncodingError {
        return AmqpFooterMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpFooter(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpFooterMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpFooter(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpFooterMarshaller.createEncoded(in);
    }

    public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpSource data) throws AmqpEncodingError {
        return AmqpSourceMarshaller.encode(data);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpSource(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSourceMarshaller.createEncoded(source, offset);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpSource(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSourceMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpSaslChallenge data) throws AmqpEncodingError {
        return AmqpSaslChallengeMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpSaslChallenge(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpSaslChallengeMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpSaslChallenge(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpSaslChallengeMarshaller.createEncoded(in);
    }

    public final Encoded<IAmqpList> encode(AmqpAttach data) throws AmqpEncodingError {
        return AmqpAttachMarshaller.encode(data);
    }

    public Encoded<IAmqpList> decodeAmqpAttach(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpAttachMarshaller.createEncoded(source, offset);
    }

    public Encoded<IAmqpList> unmarshalAmqpAttach(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpAttachMarshaller.createEncoded(in);
    }

    public final Encoded<String> encode(AmqpString data) throws AmqpEncodingError {
        return AmqpStringMarshaller.encode(data);
    }

    public Encoded<String> decodeAmqpString(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpStringMarshaller.createEncoded(source, offset);
    }

    public Encoded<String> unmarshalAmqpString(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpStringMarshaller.createEncoded(in);
    }

    public final Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpTarget data) throws AmqpEncodingError {
        return AmqpTargetMarshaller.encode(data);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpTarget(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpTargetMarshaller.createEncoded(source, offset);
    }

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpTarget(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpTargetMarshaller.createEncoded(in);
    }

    public final Encoded<Short> encode(AmqpUbyte data) throws AmqpEncodingError {
        return AmqpUbyteMarshaller.encode(data);
    }

    public Encoded<Short> decodeAmqpUbyte(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpUbyteMarshaller.createEncoded(source, offset);
    }

    public Encoded<Short> unmarshalAmqpUbyte(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpUbyteMarshaller.createEncoded(in);
    }

    public final Encoded<Long> encode(AmqpUint data) throws AmqpEncodingError {
        return AmqpUintMarshaller.encode(data);
    }

    public Encoded<Long> decodeAmqpUint(Buffer source, int offset) throws AmqpEncodingError {
        return AmqpUintMarshaller.createEncoded(source, offset);
    }

    public Encoded<Long> unmarshalAmqpUint(DataInput in) throws IOException, AmqpEncodingError {
        return AmqpUintMarshaller.createEncoded(in);
    }
}