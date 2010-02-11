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
package org.apache.activemq.amqp.protocol.marshaller;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
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

public interface AmqpMarshaller {

    /**
     * @return the protocol version of the marshaller
     */
    public AmqpVersion getVersion();

    public AmqpType<?, ?> decodeType(Buffer source) throws AmqpEncodingError;

    public AmqpType<?, ?> unmarshalType(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpSessionError data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpSessionError(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpSessionError(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Integer> encode(AmqpChar data) throws AmqpEncodingError;

    public Encoded<Integer> decodeAmqpChar(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Integer> unmarshalAmqpChar(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpUnlink data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpUnlink(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpUnlink(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpFlow data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpFlow(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpFlow(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Long> encode(AmqpLong data) throws AmqpEncodingError;

    public Encoded<Long> decodeAmqpLong(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Long> unmarshalAmqpLong(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Float> encode(AmqpFloat data) throws AmqpEncodingError;

    public Encoded<Float> decodeAmqpFloat(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Float> unmarshalAmqpFloat(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpRelink data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpRelink(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpRelink(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpClose data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpClose(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpClose(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpOpen data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpOpen(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpOpen(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Byte> encode(AmqpByte data) throws AmqpEncodingError;

    public Encoded<Byte> decodeAmqpByte(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Byte> unmarshalAmqpByte(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpRejected data) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpRejected(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpRejected(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpSaslOutcome data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpSaslOutcome(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpSaslOutcome(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Boolean> encode(AmqpBoolean data) throws AmqpEncodingError;

    public Encoded<Boolean> decodeAmqpBoolean(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Boolean> unmarshalAmqpBoolean(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<BigInteger> encode(AmqpUlong data) throws AmqpEncodingError;

    public Encoded<BigInteger> decodeAmqpUlong(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<BigInteger> unmarshalAmqpUlong(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpTransfer data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpTransfer(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpTransfer(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpDetach data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpDetach(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpDetach(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<String> encode(AmqpSymbol data) throws AmqpEncodingError;

    public Encoded<String> decodeAmqpSymbol(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<String> unmarshalAmqpSymbol(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpSaslInit data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpSaslInit(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpSaslInit(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Integer> encode(AmqpInt data) throws AmqpEncodingError;

    public Encoded<Integer> decodeAmqpInt(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Integer> unmarshalAmqpInt(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpEnlist data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpEnlist(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpEnlist(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpProperties data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpProperties(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpProperties(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Double> encode(AmqpDouble data) throws AmqpEncodingError;

    public Encoded<Double> decodeAmqpDouble(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Double> unmarshalAmqpDouble(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpDrain data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpDrain(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpDrain(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpHeader data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpHeader(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpHeader(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpSaslMechanisms data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpSaslMechanisms(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpSaslMechanisms(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<UUID> encode(AmqpUuid data) throws AmqpEncodingError;

    public Encoded<UUID> decodeAmqpUuid(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<UUID> unmarshalAmqpUuid(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpBar data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpBar(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpBar(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpFilter data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpFilter(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpFilter(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Buffer> encode(AmqpBinary data) throws AmqpEncodingError;

    public Encoded<Buffer> decodeAmqpBinary(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Buffer> unmarshalAmqpBinary(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpNoop data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpNoop(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpNoop(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpLink data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpLink(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpLink(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Date> encode(AmqpTimestamp data) throws AmqpEncodingError;

    public Encoded<Date> decodeAmqpTimestamp(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Date> unmarshalAmqpTimestamp(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Short> encode(AmqpShort data) throws AmqpEncodingError;

    public Encoded<Short> decodeAmqpShort(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Short> unmarshalAmqpShort(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpReleased data) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpReleased(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpReleased(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpTxn data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpTxn(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpTxn(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpMap data) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpMap(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpMap(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpLinkError data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpLinkError(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpLinkError(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpConnectionError data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpConnectionError(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpConnectionError(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpSaslResponse data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpSaslResponse(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpSaslResponse(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpFragment data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpFragment(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpFragment(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpDisposition data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpDisposition(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpDisposition(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Object> encode(AmqpNull data) throws AmqpEncodingError;

    public Encoded<Object> decodeAmqpNull(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Object> unmarshalAmqpNull(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpCompleted data) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpCompleted(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpCompleted(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpXid data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpXid(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpXid(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpList data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpList(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpList(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Integer> encode(AmqpUshort data) throws AmqpEncodingError;

    public Encoded<Integer> decodeAmqpUshort(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Integer> unmarshalAmqpUshort(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpFooter data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpFooter(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpFooter(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpSource data) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpSource(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpSource(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpSaslChallenge data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpSaslChallenge(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpSaslChallenge(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<IAmqpList> encode(AmqpAttach data) throws AmqpEncodingError;

    public Encoded<IAmqpList> decodeAmqpAttach(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<IAmqpList> unmarshalAmqpAttach(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<String> encode(AmqpString data) throws AmqpEncodingError;

    public Encoded<String> decodeAmqpString(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<String> unmarshalAmqpString(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> encode(AmqpTarget data) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> decodeAmqpTarget(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<HashMap<AmqpType<?,?>, AmqpType<?,?>>> unmarshalAmqpTarget(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Short> encode(AmqpUbyte data) throws AmqpEncodingError;

    public Encoded<Short> decodeAmqpUbyte(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Short> unmarshalAmqpUbyte(DataInput in) throws IOException, AmqpEncodingError;

    public Encoded<Long> encode(AmqpUint data) throws AmqpEncodingError;

    public Encoded<Long> decodeAmqpUint(Buffer source, int offset) throws AmqpEncodingError;

    public Encoded<Long> unmarshalAmqpUint(DataInput in) throws IOException, AmqpEncodingError;
}