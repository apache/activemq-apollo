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
package org.apache.activemq.amqp.generator.handcoded.types;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.amqp.generator.handcoded.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.generator.handcoded.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.generator.handcoded.marshaller.Encoded;
import org.apache.activemq.util.buffer.Buffer;

public abstract class AmqpType<E> {

    protected Encoded<E> encoded;

    public Encoded<E> getEncoded(AmqpMarshaller marshaller) throws AmqpEncodingError{
        if (encoded == null) {
            encoded = encode(marshaller);
        }
        return encoded;
    }

    public void setEncoded(Encoded<E> encoded) {
        this.encoded = encoded;
    }

    public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws AmqpEncodingError, IOException {
        Buffer b = getEncoded(marshaller).getBuffer();
        out.write(b.data, b.offset, b.length);
    }
    
    protected abstract Encoded<E> encode(AmqpMarshaller marshaller) throws AmqpEncodingError;
}