package org.apache.activemq.broker.openwire;

import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.command.Message;
import org.apache.activemq.protobuf.AsciiBuffer;

public class OpenWireMessageDelivery implements MessageDelivery {

    private final Message message;
    private Destination destination;
    private AsciiBuffer producerId;
    private Runnable completionCallback;

    public OpenWireMessageDelivery(Message message) {
        this.message = message;
    }

    public Destination getDestination() {
        if( destination == null ) {
            destination = OpenwireBrokerConnection.convert(message.getDestination());
        }
        return destination;
    }

    public int getFlowLimiterSize() {
        return message.getSize();
    }

    public int getPriority() {
        return message.getPriority();
    }

    public AsciiBuffer getMsgId() {
        return null;
    }

    public AsciiBuffer getProducerId() {
        if( producerId == null ) {
            producerId = new AsciiBuffer(message.getProducerId().toString());
        }
        return producerId;
    }

    public Message getMessage() {
        return message;
    }

    public Runnable getCompletionCallback() {
        return completionCallback;
    }

    public void setCompletionCallback(Runnable completionCallback) {
        this.completionCallback = completionCallback;
    }

    public <T> T asType(Class<T> type) {
        if( type == Message.class ) {
            return type.cast(message);
        }
        // TODO: is this right?
        if( message.getClass().isAssignableFrom(type) ) {
            return type.cast(message);
        }
        return null;
    }

}
