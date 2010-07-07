package org.apache.activemq.perf.broker.openwire;

import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createConnectionInfo;
import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createConsumerInfo;
import static org.apache.activemq.perf.broker.openwire.OpenwireSupport.createSessionInfo;

import java.io.IOException;

import org.apache.activemq.apollo.WindowLimiter;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.fusesource.hawtdispatch.Dispatch;

public class OpenwireRemoteConsumer extends RemoteConsumer {

    protected final Object inboundMutex = new Object();
    private FlowController<MessageDelivery> inboundController;

    private ActiveMQDestination activemqDestination;
    private ConnectionInfo connectionInfo;
    private SessionInfo sessionInfo;
    private ConsumerInfo consumerInfo;

    private Message lastMessage;
    private int inputWindowSize;
    private int inputResumeThreshold;

    protected void initialize() {
        inputWindowSize = 1000;
        inputResumeThreshold = 500;
        // Setup the input processing..
        final Flow flow = new Flow("client-" + name + "-inbound", false);
        inputResumeThreshold = inputWindowSize / 2;
        WindowLimiter<MessageDelivery> limiter = new WindowLimiter<MessageDelivery>(false, flow, inputWindowSize, inputResumeThreshold) {
            @Override
            protected void sendCredit(int credit) {
                MessageAck ack = OpenwireSupport.createAck(consumerInfo, lastMessage, credit, MessageAck.STANDARD_ACK_TYPE);
                write(ack);
            }

            public int getElementSize(MessageDelivery md) {
                return 1;
            }
        };
        inboundController = new FlowController<MessageDelivery>(new FlowControllable<MessageDelivery>() {
            public void flowElemAccepted(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
                messageReceived(controller, elem);
            }

            public String toString() {
                return flow.getFlowName();
            }

            public IFlowResource getFlowResource() {
                return null;
            }
        }, flow, limiter, inboundMutex);
        inboundController.setExecutor(Dispatch.getGlobalQueue());

    }

    protected void setupSubscription() throws Exception, IOException {
        if (destination.getDomain().equals(Router.QUEUE_DOMAIN)) {
            activemqDestination = new ActiveMQQueue(destination.getName().toString());
        } else {
            activemqDestination = new ActiveMQTopic(destination.getName().toString());
        }

        connectionInfo = createConnectionInfo(name);
        transport.oneway(connectionInfo, null);
        sessionInfo = createSessionInfo(connectionInfo);
        transport.oneway(sessionInfo, null);
        consumerInfo = createConsumerInfo(sessionInfo, activemqDestination, isDurable() ? name : null);
        consumerInfo.setPrefetchSize(inputWindowSize);
        transport.oneway(consumerInfo, null);
    }

    public void onCommand(Object command) {
        try {
            if (command.getClass() == WireFormatInfo.class) {
            } else if (command.getClass() == BrokerInfo.class) {
                //System.out.println("Consumer "+name+" connected to "+((BrokerInfo)command).getBrokerName());
            } else if (command.getClass() == MessageDispatch.class) {
                MessageDispatch msg = (MessageDispatch) command;
                lastMessage = msg.getMessage();
                inboundController.add(new OpenWireMessageDelivery(msg.getMessage()), null);
            } else {
                onException(new Exception("Unrecognized command: " + command));
            }
        } catch (Exception e) {
            onException(e);
        }
    }
}