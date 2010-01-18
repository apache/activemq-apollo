package org.apache.activemq.amqp.generator.handcoded.types;

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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class AmqpType {

    protected int encodedSize;
    protected int encodedCount;
    
    public static enum FormatCategory {
        FIXED(false, false), VARIABLE(false, false), COMPOUND(false, false), ARRAY(false, false);

        private final boolean encodesSize;
        private final boolean encodesCount;

        FormatCategory(boolean encodesSize, boolean encodesCount) {
            this.encodesSize = encodesSize;
            this.encodesCount = encodesCount;
        }

        public static FormatCategory getCategory(byte formatCode) throws IllegalArgumentException {
            switch ((byte) (formatCode | 0xF0)) {
            case (byte) 0x40:
            case (byte) 0x50:
            case (byte) 0x60:
            case (byte) 0x70:
            case (byte) 0x80:
            case (byte) 0x90:
                return FIXED;
            case (byte) 0xA0:
            case (byte) 0xB0:
                return VARIABLE;
            case (byte) 0xC0:
            case (byte) 0xD0:
                return COMPOUND;
            case (byte) 0xE0:
            case (byte) 0xF0:
                return ARRAY;
            default:
                throw new IllegalArgumentException("" + formatCode);
            }
        }

        public final boolean encodesSize() {
            return encodesSize;
        }

        public final boolean encodesCount() {
            return encodesCount;
        }
    }

    public static enum FormatSubCategory {
        FIXED_0((byte) 0x40, 0), FIXED_1((byte) 0x50, 1), FIXED_2((byte) 0x60, 2), FIXED_4((byte) 0x70, 4), FIXED_8((byte) 0x80, 8), FIXED_16((byte) 0x90, 16), VARIABLE_1((byte) 0xA0, 1), VARIABLE_4(
                (byte) 0xB0, 4), COMPOUND_1((byte) 0xC0, 1), COMPOUND_4((byte) 0xD0, 4), ARRAY_1((byte) 0xE0, 1), ARRAY_4((byte) 0xF0, 4);

        private final FormatCategory category;
        private final byte subCategory;
        public final int WIDTH;

        FormatSubCategory(byte subCategory, int width) {
            this.subCategory = subCategory;
            category = FormatCategory.getCategory(this.subCategory);
            this.WIDTH = width;
        }

        public static FormatSubCategory getCategory(byte formatCode) throws IllegalArgumentException {
            switch ((byte) (formatCode | 0xF0)) {
            case (byte) 0x40:
                return FIXED_0;
            case (byte) 0x50:
                return FIXED_1;
            case (byte) 0x60:
                return FIXED_2;
            case (byte) 0x70:
                return FIXED_4;
            case (byte) 0x80:
                return FIXED_8;
            case (byte) 0x90:
                return FIXED_16;
            case (byte) 0xA0:
                return VARIABLE_1;
            case (byte) 0xB0:
                return VARIABLE_4;
            case (byte) 0xC0:
                return COMPOUND_1;
            case (byte) 0xD0:
                return COMPOUND_4;
            case (byte) 0xE0:
                return ARRAY_1;
            case (byte) 0xF0:
                return ARRAY_4;
            default:
                throw new IllegalArgumentException("" + formatCode);
            }
        }

        public final void marshalFormatHeader(AmqpType type, DataOutputStream dos) throws IOException {
            if (category == FormatCategory.FIXED) {
                return;
            }

            if (category.encodesSize()) {
                if (WIDTH == 1) {
                    dos.writeByte(type.getEncodedSize());
                } else {
                    dos.writeInt(type.getEncodedCount());
                }
            }

            if (category.encodesCount()) {
                if (WIDTH == 1) {
                    dos.writeByte(type.getEncodedCount());
                } else {
                    dos.writeInt(type.getEncodedCount());
                }
            }
        }

        public final void unmarshalFormatHeader(AmqpType type, DataInputStream dis) throws IOException {
            if (category == FormatCategory.FIXED) {
                return;
            }

            if (category.encodesSize()) {
                if (WIDTH == 1) {
                    type.setEncodedSize(dis.readByte());
                } else {
                    type.setEncodedCount(dis.readInt());
                }
            }

            if (category.encodesCount()) {
                if (WIDTH == 1) {
                    type.setEncodedCount(dis.readByte());
                } else {
                    type.setEncodedCount(dis.readInt());
                }
            }
        }
    }
    
    protected void setEncodedSize(int size) {
        encodedSize = size;
    }

    protected void setEncodedCount(int count) throws IOException {
        encodedCount = count;
    }

    public abstract int getEncodedSize() throws IOException;

    public abstract int getEncodedCount() throws IOException;

    public abstract void unmarshal(DataInputStream dis) throws IOException;
    
    public abstract void marshal(DataOutputStream dos) throws IOException;

    public abstract void marshalConstructor(DataOutputStream dos) throws IOException;

    public abstract void marshalData(DataOutputStream dos) throws IOException;
}
