package org.apache.activemq.amqp.wireformat;

import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.types.*;
import org.fusesource.hawtbuf.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class AmqpWireFormatFactory implements WireFormatFactory {

    private static final Buffer MAGIC = new Buffer(new byte[] { 'A', 'M', 'Q', 'P' });

    public WireFormat createWireFormat() {
        return new AmqpWireFormat(1);
    }

    public boolean isDiscriminatable() {
        return true;
    }

    public boolean matchesWireformatHeader(Buffer buffer) {
        if (buffer.length >= MAGIC.length) {
            return buffer.containsAt(MAGIC, 0);
        }
        return false;
    }

    public int maxWireformatHeaderLength() {
        return 8;
    }
}
