package org.apache.activemq.broker;

import org.apache.activemq.protobuf.AsciiBuffer;

public interface MessageDelivery {

    public Destination getDestination();

    public int getPriority();

    public int getFlowLimiterSize();

    public AsciiBuffer getMsgId();

    public AsciiBuffer getProducerId();

    public void setCompletionCallback(Runnable runnable);
    public Runnable getCompletionCallback();

    public <T> T asType(Class<T> type);

}
