package org.apache.activemq.perf.broker.openwire;

import java.util.concurrent.atomic.AtomicLong;

import javax.jms.MessageNotWriteableException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.SessionInfo;

public class OpenwireSupport {
    
    static private long idGenerator;
    static private AtomicLong msgIdGenerator = new AtomicLong(0);

    public static ConsumerInfo createConsumerInfo(SessionInfo sessionInfo, ActiveMQDestination destination, String subscriptionName) throws Exception {
        ConsumerInfo info = new ConsumerInfo(sessionInfo, ++idGenerator);
        info.setBrowser(false);
        info.setDestination(destination);
        info.setPrefetchSize(1000);
        info.setDispatchAsync(false);
        info.setSubscriptionName(subscriptionName);
        return info;
    }

    public static RemoveInfo closeConsumerInfo(ConsumerInfo consumerInfo) {
        return consumerInfo.createRemoveCommand();
    }

    public static ProducerInfo createProducerInfo(SessionInfo sessionInfo) throws Exception {
        ProducerInfo info = new ProducerInfo(sessionInfo, ++idGenerator);
        return info;
    }

    public static SessionInfo createSessionInfo(ConnectionInfo connectionInfo) throws Exception {
        SessionInfo info = new SessionInfo(connectionInfo, ++idGenerator);
        return info;
    }

    
    public static ConnectionInfo createConnectionInfo() throws Exception {
        return createConnectionInfo("connection:"+ (++idGenerator));
    }

    public static ConnectionInfo createConnectionInfo(String name) throws Exception {
        ConnectionInfo info = new ConnectionInfo();
        info.setConnectionId(new ConnectionId(name));
        info.setClientId(info.getConnectionId().getValue());
        return info;
    }

    public static ActiveMQTextMessage createMessage(ProducerInfo producerInfo, ActiveMQDestination destination) {
        return createMessage(producerInfo, destination, 4, null);
    }
    
    public static ActiveMQTextMessage createMessage(ProducerInfo producerInfo, ActiveMQDestination destination, int priority, String payload) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setJMSPriority(priority);
        message.setProducerId(producerInfo.getProducerId());
        message.setMessageId(new MessageId(producerInfo, msgIdGenerator.incrementAndGet()));
        message.setDestination(destination);
        message.setPersistent(false);
        if( payload!=null ) {
            try {
                message.setText(payload);
            } catch (MessageNotWriteableException e) {
            }
        }
        return message;
    }

    public static MessageAck createAck(ConsumerInfo consumerInfo, Message msg, int count, byte ackType) {
        MessageAck ack = new MessageAck();
        ack.setAckType(ackType);
        ack.setConsumerId(consumerInfo.getConsumerId());
        ack.setDestination(msg.getDestination());
        ack.setLastMessageId(msg.getMessageId());
        ack.setMessageCount(count);
        return ack;
    }

}
