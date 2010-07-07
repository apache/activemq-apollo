package org.apache.activemq.amqp.broker;

import java.io.IOException;

import org.apache.activemq.amqp.wireformat.AmqpWireFormat;
import org.apache.activemq.amqp.protocol.types.AmqpTransfer;
import org.apache.activemq.apollo.broker.BrokerMessageDelivery;
import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.filter.MessageEvaluationContext;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;

public class AmqpMessageDelivery extends BrokerMessageDelivery {

    static final private AsciiBuffer ENCODING = new AsciiBuffer("openwire");
    private AmqpWireFormat storeWireFormat;
    private AmqpTransfer message;

    AmqpMessageDelivery(AmqpTransfer message) {
        this.message = message;

    }

    @Override
    public int getMemorySize() {
        // TODO
        return 0;
    }

    @Override
    protected Buffer getStoreEncoded() {
        Buffer bytes;
        try {
            bytes = storeWireFormat.marshal(message);
        } catch (IOException e) {
            return null;
        }
        return bytes;
    }

    @Override
    protected AsciiBuffer getStoreEncoding() {
        return ENCODING;
    }

    public <T> T asType(Class<T> type) {
        if (type == AmqpTransfer.class) {
            return type.cast(message);
        }
        // TODO: is this right?
        if (message.getClass().isAssignableFrom(type)) {
            return type.cast(message);
        }
        return null;
    }

    public MessageEvaluationContext createMessageEvaluationContext() {
        // TODO Auto-generated method stub
        return null;
    }

    public Destination getDestination() {
        // TODO Auto-generated method stub
        return null;
    }

    public long getExpiration() {
        // TODO Auto-generated method stub
        return -1;
    }

    public AsciiBuffer getMsgId() {
        // TODO Auto-generated method stub
        return null;
    }

    public int getPriority() {
        // TODO
        return 0;
    }

    public AsciiBuffer getProducerId() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isPersistent() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isResponseRequired() {
        // TODO Auto-generated method stub
        return false;
    }

    public void onMessagePersisted() {
        // TODO Auto-generated method stub

    }

    public void setStoreWireFormat(AmqpWireFormat format) {
        this.storeWireFormat = format;
    }

}
