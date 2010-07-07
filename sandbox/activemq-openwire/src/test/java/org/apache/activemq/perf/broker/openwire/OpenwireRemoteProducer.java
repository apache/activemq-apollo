package org.apache.activemq.perf.broker.openwire;

import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createConnectionInfo;
import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createMessage;
import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createProducerInfo;
import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createSessionInfo;

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.activemq.apollo.WindowLimiter;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.broker.RemoteProducer;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.SingleFlowRelay;

public class OpenwireRemoteProducer extends RemoteProducer {
    private ConnectionInfo connectionInfo;
    private SessionInfo sessionInfo;
    private ProducerInfo producerInfo;
    private ActiveMQDestination activemqDestination;
    private WindowLimiter<MessageDelivery> outboundLimiter;
    private int outputWindowSize = 1024*4;
    private int outputResumeThreshold = 1024*4;

    protected void setupProducer() throws Exception, IOException {
        if (destination.getDomain().equals(Router.QUEUE_DOMAIN)) {
            activemqDestination = new ActiveMQQueue(destination.getName().toString());
        } else {
            activemqDestination = new ActiveMQTopic(destination.getName().toString());
        }

        connectionInfo = createConnectionInfo(name);
        transport.oneway(connectionInfo, null);
        sessionInfo = createSessionInfo(connectionInfo);
        transport.oneway(sessionInfo, null);
        producerInfo = createProducerInfo(sessionInfo);
        producerInfo.setWindowSize(outputWindowSize);
        transport.oneway(producerInfo, null);
    }

    protected void initialize() {
        Flow flow = new Flow("client-" + name + "-outbound", false);
        outputResumeThreshold = outputWindowSize / 2;
        outboundLimiter = new WindowLimiter<MessageDelivery>(true, flow, outputWindowSize, outputResumeThreshold);
        SingleFlowRelay<MessageDelivery> outboundQueue = new SingleFlowRelay<MessageDelivery>(flow, flow.getFlowName(), outboundLimiter);
        this.outboundQueue = outboundQueue;

        outboundController = outboundQueue.getFlowController(flow);
        outboundQueue.setDrain(new QueueDispatchTarget<MessageDelivery>() {
            public void drain(MessageDelivery message, ISourceController<MessageDelivery> controller) {
                Message msg = message.asType(Message.class);
                write(msg);
            }
        });
    }

    public void onCommand(Object command) {
        try {
            if (command.getClass() == WireFormatInfo.class) {
            } else if (command.getClass() == BrokerInfo.class) {
                //System.out.println("Producer " + name + " connected to " + ((BrokerInfo) command).getBrokerName());
            } else if (command.getClass() == ProducerAck.class) {
                ProducerAck fc = (ProducerAck) command;
                synchronized (outboundQueue) {
                    outboundLimiter.onProtocolCredit(fc.getSize());
                }
            } else {
                onException(new Exception("Unrecognized command: " + command));
            }
        } catch (Exception e) {
            onException(e);
        }
    }

    protected void createNextMessage() {
        int priority = this.priority;
        if (priorityMod > 0) {
            priority = counter % priorityMod == 0 ? 0 : priority;
        }

        ActiveMQTextMessage msg = createMessage(producerInfo, activemqDestination, priority, createPayload());
        if (persistentDelivery) {
            msg.setPersistent(true);
        }
        if (property != null) {
            try {
                msg.setStringProperty(property, property);
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
        
        //Call the before marshal method to sync the content so we get an 
        //accurate size:
        try {
            msg.beforeMarshall(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        next = new OpenWireMessageDelivery(msg);
    }
}
