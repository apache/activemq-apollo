/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with his work
 * for additional information regarding copyright ownership. The ASF licenses
 * this file to You under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.activemq.amqp.protocol;

/**
 * <p>
 * Frames are divided into two distinct areas: a fixed width frame header, and a
 * variable width frame body.
 * </p>
 * <p>
 * <![CDATA[ +--------------+------------+ | frame header | frame body |
 * +--------------+------------+ 24 bytes *variable* ]]>
 * </p>
 * <dl>
 * <dt>frame header</dt>
 * <dd>
 * <p>
 * The frame header is a fixed size (24 byte) structure that precedes each
 * frame. The frame header includes information required to parse the rest of
 * the frame.
 * </p>
 * </dd>
 * <dt>frame body</dt>
 * <dd>
 * <p>
 * The frame body is a variable width sequence of bytes whose interpretation
 * depends on the frame type. For Connection and Session frames, this will, when
 * non-empty, contain an encoded control or command.
 * </p>
 * </dd>
 * </dl>
 * <p>
 * <b>Frame Header</b>
 * <p>
 * The first 8 bytes of the frame header are formatted identically for all frame
 * types. The semantics of the frame flags, and the format of the last 16 bytes
 * of the frame header are dependent on the specific type of frame as indicated
 * by the value of the type field. There are two frame types defined: Connection
 * frames and Session frames.
 * </p>
 * <p>
 * 
 * <pre>
 *      +0         +1         +2         +3
 *      +-------------------------------------------+ -.
 *    0 |                   size                    |  |
 *      +-------------------------------------------+  |
 *    4 |  type  |   flags    |       channel       |  |
 *      +-------------------------------------------+  |
 *    8 |             type-dependent                |  |
 *      +-------------------------------------------+  |---> frame header
 *   12 |             type-dependent                |  |      (24 bytes)
 *      +-------------------------------------------+  |
 *   16 |             type-dependent                |  |
 *      +-------------------------------------------+  |
 *   20 |             type-dependent                |  |
 *      +-------------------------------------------+ -'
 *      +-------------------------------------------+ -.
 *      |                    ...                    |  |
 *      .                                           .  |
 *      .                                           .  |---> frame body
 *      .                                           .  |  (size - 24) bytes
 *      |                    ...                    |  |
 *      +-------------------------------------------+ -'
 * </pre>
 * 
 * </p>
 * <dl>
 * <dt>size</dt>
 * <dd>
 * <p>
 * Bytes 0-3 of the frame header contain the frame size. This is an unsigned
 * 32-bit integer that MUST contain the total frame size including the frame
 * header. The frame is malformed if the size is less than the the size of the
 * header (24 bytes).
 * </p>
 * </dd>
 * <dt>type</dt>
 * <dd>
 * <p>
 * Byte 4 of the frame header is a type code. The type code indicates the format
 * and purpose of the frame. A type code of 0x00 indicates that the frame is a
 * Connection frame. A type code of 0x01 indicates that the frame is a Session
 * frame. The subsequent bytes in the frame header may be interpreted
 * differently depending on the type of the frame.
 * </p>
 * </dd>
 * <dt>flags</dt>
 * <dd>
 * <p>
 * Byte 5 of the frame header is reserved for frame flags. The semantics of the
 * frame flags are determined by the frame type.
 * </p>
 * </dd>
 * <dt>channel</dt>
 * <dd>
 * <p>
 * Bytes 6 and 7 of the frame header contain the channel number. The channel
 * number uniquely identifies one of the Sessions associated with the
 * Connection.
 * </p>
 * </dd>
 * </dl>
 * 
 * <b>Frame Body</b>
 * <p>
 * The frame body contains a variable number of octets, anywhere from zero, up
 * to the maximum frame size minus 24 bytes for the header. Prior to any
 * explicit negotiation, the maximum frame size is <xref
 * name="MIN-MAX-FRAME-SIZE"/>. For Connection and Session frames, the frame
 * body, if non-empty, will consist of a command or control operation encoded as
 * a compound value (see <xref name="compound-types"/>). The complete set of
 * compound type definitions for the core AMQP protocol commands and controls
 * are defined in the "commands"and "controls" sections respectively.
 * </p>
 */
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!//
//!!!!!!!!THIS CLASS IS AUTOGENERATED DO NOT MODIFY DIRECTLY!!!!!!!!!!!!//
//!!!!!!Instead, modify the generator in activemq-amqp-generator!!!!!!!!//
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!//
public class AmqpFrame {

    protected byte[] frameHeader;

    protected static final int SIZE_OFFSET = 0;
    protected static final int TYPE_OFFSET = 4;
    protected static final int FLAGS_OFFSET = 5;
    protected static final int CHANNEL_OFFSET = 6;

    public static final byte CONNECTION_TYPE = 0x00;
    public static final byte SESSION_TYPE = 0x01;

    public static enum FrameType {

        CONNECTION(CONNECTION_TYPE), SESSION(SESSION_TYPE);
        final byte type;

        FrameType(byte type) {
            this.type = type;
        }

        public static FrameType getFrameType(AmqpFrame f) throws AmqpFramingException {
            switch (f.frameHeader[TYPE_OFFSET]) {
            case CONNECTION_TYPE:
                return CONNECTION;
            case SESSION_TYPE:
                return SESSION;
            default:
                throw new AmqpFramingException("Unknown Frame Type: " + f.frameHeader[TYPE_OFFSET]);
            }
        }
    }

    public final FrameType getFrameType() throws AmqpFramingException {
        return FrameType.getFrameType(this);
    }

    public final void setSize(long size) {
        BitUtils.setUInt(frameHeader, SIZE_OFFSET, size);
    }

    public long getSize() {
        return BitUtils.getUInt(frameHeader, SIZE_OFFSET);
    }

    protected void setFlags(byte flags) {
        frameHeader[FLAGS_OFFSET] = flags;
    }

    public final void setChannel(int channel) {
        BitUtils.setUShort(frameHeader, CHANNEL_OFFSET, channel);
    }
    
    public final int getChannel() {
        return BitUtils.getUShort(frameHeader, CHANNEL_OFFSET);
    }
}
