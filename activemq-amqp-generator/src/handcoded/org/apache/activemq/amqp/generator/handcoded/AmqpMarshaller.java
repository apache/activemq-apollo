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
package org.apache.activemq.amqp.generator.handcoded;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.amqp.generator.handcoded.types.AmqpType;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpBinary;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpBoolean;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpList;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpMap;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpString;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpSymbol;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpBinary.BINARY_ENCODING;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpBoolean.BOOLEAN_ENCODING;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpList.LIST_ENCODING;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpMap.MAP_ENCODING;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpString.STRING_ENCODING;
import org.apache.activemq.amqp.generator.handcoded.types.AmqpSymbol.SYMBOL_ENCODING;

public class AmqpMarshaller {

    public static final AmqpBinary.BINARY_ENCODING chooseBinaryEncoding(byte[] val) throws IOException {
        if (val.length > 255) {
            return AmqpBinary.BINARY_ENCODING.VBIN32;
        }
        return AmqpBinary.BINARY_ENCODING.VBIN8;
    }

    public static final AmqpBoolean.BOOLEAN_ENCODING chooseBooleanEncoding(boolean val) throws IOException {
        if (val) {
            return AmqpBoolean.BOOLEAN_ENCODING.TRUE;
        }
        return AmqpBoolean.BOOLEAN_ENCODING.FALSE;
    }

    public static final AmqpList.LIST_ENCODING chooseListEncoding(List<AmqpType> val) throws IOException {
        if (val.size() > 255) {
            return AmqpList.LIST_ENCODING.LIST32;
        }
        for (AmqpType le : val) {
            int size = le.getEncodedSize();
            if (size > 255) {
                return AmqpList.LIST_ENCODING.LIST32;
            }
        }
        return AmqpList.LIST_ENCODING.LIST8;
    }

    public static final AmqpMap.MAP_ENCODING chooseMapEncoding(HashMap<AmqpType, AmqpType> val) throws IOException {
        for (Map.Entry<AmqpType, AmqpType> me : val.entrySet()) {
            int size = me.getKey().getEncodedSize() + me.getValue().getEncodedSize();
            if (size > 255) {
                return AmqpMap.MAP_ENCODING.MAP32;
            }
        }
        return AmqpMap.MAP_ENCODING.MAP8;
    }

    public static final AmqpString.STRING_ENCODING chooseStringEncoding(String val) throws IOException {
        if (val.length() > 255 || val.getBytes("utf-16").length > 255) {
            return AmqpString.STRING_ENCODING.STR32_UTF16;
        }

        return AmqpString.STRING_ENCODING.STR32_UTF16;
    }

    public static final AmqpSymbol.SYMBOL_ENCODING chooseSymbolEncoding(String val) throws IOException {
        if (val.length() > 255 || val.getBytes("ascii").length > 255) {
            return AmqpSymbol.SYMBOL_ENCODING.SYM32;
        }
        return AmqpSymbol.SYMBOL_ENCODING.SYM8;
    }

    public static int getEncodedCounfOfList(List<AmqpType> val, LIST_ENCODING listENCODING) {
        return val.size();
    }

    public static int getEncodedCounfOfMap(HashMap<AmqpType, AmqpType> val, MAP_ENCODING mapENCODING) {
        return val.size() * 2;
    }

    public static final int getEncodedSizeOfBinary(byte[] val, BINARY_ENCODING encoding) throws IOException {
        return val.length;
    }

    public static final int getEncodedSizeOfBoolean(boolean val, BOOLEAN_ENCODING encoding) throws IOException {
        return 0;
    }

    public static final int getEncodedSizeOfList(List<AmqpType> val, LIST_ENCODING encoding) throws IOException {
        int size = 0;
        switch (encoding) {
        case ARRAY32: {
            size = 4;
            for (AmqpType le : val) {
                size += le.getEncodedSize();
            }
            return size;
        }
        case ARRAY8: {
            size = 1;
            for (AmqpType le : val) {
                size += le.getEncodedSize();
            }
            return size;
        }
        case LIST32: {
            size = 8;
            for (AmqpType le : val) {
                size += le.getEncodedSize();
            }
            return size;
        }
        case LIST8: {
            size = 8;
            for (AmqpType le : val) {
                size += le.getEncodedSize();
            }
            return size;
        }
        default: {
            throw new UnsupportedEncodingException();
        }
        }
    }

    public static final int getEncodedSizeOfMap(HashMap<AmqpType, AmqpType> val, MAP_ENCODING encoding) throws IOException {
        int size = 0;
        for (Map.Entry<AmqpType, AmqpType> me : val.entrySet()) {
            size += me.getKey().getEncodedSize() + me.getValue().getEncodedSize();
        }
        return size;
    }

    public static final int getEncodedSizeOfString(String val, STRING_ENCODING encoding) throws IOException {
        switch (encoding) {
        case STR32_UTF16:
        case STR8_UTF16: {
            return val.getBytes("utf-16").length;
        }
        case STR32_UTF8:
        case STR8_UTF8: {
            return val.getBytes("utf-8").length;
        }
        default:
            throw new UnsupportedEncodingException(encoding.name());
        }
    }

    public static final int getEncodedSizeOfSymbol(String val, SYMBOL_ENCODING encoding) throws IOException {
        return val.length();
    }

    public static final byte[] readBinary(AmqpBinary.BINARY_ENCODING encoding, int length, int count, DataInput dis) throws IOException {
        byte[] rc = new byte[length];
        dis.readFully(rc);
        return rc;
    }

    public static final byte readByte(DataInput dis) throws IOException {
        return (byte) dis.readByte();
    }

    public static final int readChar(DataInput dis) throws IOException {
        return dis.readInt();
    }

    public static final double readDouble(DataInput dis) throws IOException {
        return dis.readDouble();
    }

    public static final float readFloat(DataInput dis) throws IOException {
        return dis.readFloat();
    }

    public static final int readInt(DataInput dis) throws IOException {
        return dis.readInt();
    }

    public static final List<AmqpType> readList(AmqpList.LIST_ENCODING encoding, int size, int count, DataInput dis) throws IOException {
        List<AmqpType> rc = new ArrayList<AmqpType>(count);
        for (int i = 0; i < count; i++) {
            rc.set(i, readType(dis));
        }
        return rc;
    }

    public static final long readLong(DataInput dis) throws IOException {
        return dis.readLong();
    }

    public static final HashMap<AmqpType, AmqpType> readMap(AmqpMap.MAP_ENCODING encoding, int size, int count, DataInput dis) throws IOException {
        HashMap<AmqpType, AmqpType> rc = new HashMap<AmqpType, AmqpType>();
        for (int i = 0; i < count; i++) {
            rc.put(readType(dis), readType(dis));
        }
        return rc;
    }

    public static final short readShort(DataInput dis) throws IOException {
        return dis.readShort();
    }

    public static final String readString(AmqpString.STRING_ENCODING encoding, int size, int count, DataInput dis) throws IOException {
        byte[] str = new byte[size];
        dis.readFully(str);
        switch (encoding) {
        case STR32_UTF16:
        case STR8_UTF16:
            return new String(str, "utf-16");
        case STR32_UTF8:
        case STR8_UTF8:
            return new String(str, "utf-8");
        default:
            throw new UnsupportedEncodingException(encoding.name());
        }
    }

    public static final String readSymbol(AmqpSymbol.SYMBOL_ENCODING encoding, int size, int count, DataInput dis) throws IOException {
        byte[] str = new byte[size];
        dis.readFully(str);
        return new String(str, "ascii");
    }

    public static final Date readTimestamp(DataInput dis) throws IOException {
        return new Date(dis.readInt());
    }

    public static final short readUbyte(DataInput dis) throws IOException {
        return (short) (0xFF & (short) dis.readByte());
    }

    public static final long readUint(DataInput dis) throws IOException {
        long rc = 0;
        rc = rc | (0xFFFFFFFFL & (((long) dis.readByte()) << 24));
        rc = rc | (0xFFFFFFFFL & (((long) dis.readByte()) << 16));
        rc = rc | (0xFFFFFFFFL & (((long) dis.readByte()) << 8));
        rc = rc | (0xFFFFFFFFL & (long) dis.readByte());

        return rc;
    }

    public static final BigInteger readUlong(DataInput dis) throws IOException {
        byte[] rc = new byte[9];
        rc[0] = 0;
        dis.readFully(rc, 1, 8);
        return new BigInteger(rc);
    }

    public static final int readUshort(DataInput dis) throws IOException {
        int rc = 0;
        rc = rc | ((int) 0xFFFF & (((int) dis.readByte()) << 8));
        rc = rc | ((int) 0xFFFF & (int) dis.readByte());

        return rc;
    }

    public static final UUID readUuid(DataInput dis) throws IOException {
        return new UUID(dis.readLong(), dis.readLong());
    }

    public static final void writeBinary(byte[] val, AmqpBinary.BINARY_ENCODING encoding, DataOutput dos) throws IOException {
        dos.write(val);
    }

    public static final void writeByte(byte val, DataOutput dos) throws IOException {
        dos.writeByte(val);
    }

    public static final void writeChar(int val, DataOutput dos) throws IOException {
        dos.writeInt(val);

    }

    public static final void writeDouble(double val, DataOutput dos) throws IOException {
        dos.writeLong(Double.doubleToLongBits(val));
    }

    public static final void writeFloat(float val, DataOutput dos) throws IOException {
        dos.writeInt(Float.floatToIntBits(val));
    }

    public static final void writeInt(int val, DataOutput dos) throws IOException {
        dos.writeInt(val);
    }

    public static final void writeList(List<AmqpType> val, AmqpList.LIST_ENCODING encoding, DataOutput dos) throws IOException {
        switch (encoding) {
        case ARRAY32:
        case ARRAY8: {
            val.get(0).marshalConstructor(dos);
            for (AmqpType le : val) {
                le.marshalData(dos);
            }
        }
        case LIST32:
        case LIST8: {
            for (AmqpType le : val) {
                le.marshal(dos);
            }
        }
        default: {
            throw new UnsupportedEncodingException();
        }
        }
    }

    public static final void writeLong(long val, DataOutput dos) throws IOException {
        dos.writeLong(val);
    }

    public static final void writeMap(HashMap<AmqpType, AmqpType> val, AmqpMap.MAP_ENCODING encoding, DataOutput dos) throws IOException {
        for (Map.Entry<AmqpType, AmqpType> me : val.entrySet()) {
            me.getKey().marshal(dos);
            me.getValue().marshal(dos);
        }
    }

    public static final void writeShort(short val, DataOutput dos) throws IOException {
        dos.writeShort(val);
    }

    public static final void writeString(String val, AmqpString.STRING_ENCODING encoding, DataOutput dos) throws IOException {
        switch (encoding) {
        case STR32_UTF16:
        case STR8_UTF16: {
            dos.write(val.getBytes("utf-16"));
        }
        case STR32_UTF8:
        case STR8_UTF8: {
            dos.write(val.getBytes("utf-8"));
        }
        default:
            throw new UnsupportedEncodingException(encoding.name());
        }

    }

    public static final void writeSymbol(String val, SYMBOL_ENCODING encoding, DataOutput dos) throws IOException {
        dos.write(val.getBytes("ascii"));
    }

    public static final void writeTimestamp(Date val, DataOutput dos) throws IOException {
        dos.writeInt((int) val.getTime());

    }

    public static final void writeUbyte(short val, DataOutput dos) throws IOException {
        dos.write(val);

    }

    public static final void writeUint(long val, DataOutput dos) throws IOException {
        dos.writeInt((int) val);

    }

    public static final void writeUlong(BigInteger val, DataOutput dos) throws IOException {
        byte[] b = val.toByteArray();
        if (b.length > 8) {
            for (int i = 0; i < b.length - 8; i++) {
                if (b[i] > 0) {
                    throw new UnsupportedEncodingException("Unsigned long too large");
                }
            }
        }
        dos.write(b, b.length - 8, 8);
    }

    public static final void writeUshort(int val, DataOutput dos) throws IOException {
        dos.writeShort((short) val);
    }

    public static final void writeUuid(UUID val, DataOutput dos) throws IOException {
        dos.writeLong(val.getMostSignificantBits());
        dos.writeLong(val.getLeastSignificantBits());
    }
    
    public static final AmqpType readType(DataInput dis) throws IOException
    {
        //TODO 
        return null;
    }

    // public static final void main(String[] arg) {
    // AmqpMarshaller marshaller = new AmqpMarshaller();
    //
    // ByteArrayOutputStream baos = new ByteArrayOutputStream(30);
    // DataOutput dos = new DataOutput(baos);
    //
    // try {
    // BigInteger unsigned = new BigInteger("" + Long.MAX_VALUE).multiply(new
    // BigInteger("2"));
    // marshaller.writeUlong(unsigned, dos);
    // dos.flush();
    //
    // byte[] b = baos.toByteArray();
    // ByteArrayInputStream bais = new ByteArrayInputStream(b);
    // DataInput dis = new DataInput(bais);
    // BigInteger read = marshaller.readUlong(dis);
    // if (read != unsigned) {
    // throw new Exception();
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }
}
