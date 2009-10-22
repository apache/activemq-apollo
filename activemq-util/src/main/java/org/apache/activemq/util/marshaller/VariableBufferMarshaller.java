/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.activemq.util.marshaller;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.util.buffer.Buffer;

/**
 * Implementation of a Marshaller for Buffer objects
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class VariableBufferMarshaller extends VariableMarshaller<Buffer> {

    public void writePayload(Buffer value, DataOutput dataOut) throws IOException {
        dataOut.writeInt(value.length);
        dataOut.write(value.data, value.offset, value.length);
    }

    public Buffer readPayload(DataInput dataIn) throws IOException {
        int size = dataIn.readInt();
        byte[] data = new byte[size];
        dataIn.readFully(data);
        return new Buffer(data);
    }
    
    public Buffer deepCopy(Buffer source) {
        return source.deepCopy();
    }

    public boolean isDeepCopySupported() {
        return true;
    }

    public int estimatedSize(Buffer object) {
        return object.length+4;
    }
    
}
