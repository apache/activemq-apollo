package org.apache.activemq.amqp.wireformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.amqp.protocol.Definitions;
import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.types.AmqpType;
import org.apache.activemq.transport.Transport;
import org.fusesource.hawtbuf.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class AmqpWireFormat implements WireFormat {

    public static final int DEFAULT_VERSION = 1;
    public static final String WIREFORMAT_NAME = "amqp";
    private int version;
    private AtomicBoolean receivingMessage = new AtomicBoolean(false);

    private long maxFrameSize = Long.valueOf(Definitions.MIN_MAX_FRAME_SIZE);
    private long heartBeatInterval;

    private AmqpMarshaller marshaller;

    AmqpWireFormat(int version) {
        this.version = version;
    }

    @SuppressWarnings("unchecked")
    public Transport createTransportFilters(Transport transport, Map options) {
        return transport;
    }

    public String getName() {
        return WIREFORMAT_NAME;
    }

    public void setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public long getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public void setHeartBeatInterval(long heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
    }

    public int getVersion() {
        return version;
    }

    public WireFormatFactory getWireFormatFactory() {
        return new AmqpWireFormatFactory();
    }

    public boolean inReceive() {
        return receivingMessage.get();
    }

    public Buffer marshal(Object command) throws IOException {
        return ((AmqpType<?, ?>) command).getBuffer(marshaller).getEncoded().getBuffer();
    }

    public void marshal(Object command, DataOutput out) throws IOException {
        ((AmqpType<?, ?>) command).marshal(out, marshaller);
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Object unmarshal(Buffer buffer) throws IOException {
        try {
            return marshaller.decodeType(buffer);
        } catch (AmqpEncodingError error) {
            throw new IOException(error.getMessage(), error);
        }
    }

    public Object unmarshal(DataInput in) throws IOException {
        receivingMessage.set(true);
        try {
            return marshaller.unmarshalType(in);
        } finally {
            receivingMessage.set(false);
        }
    }

}
