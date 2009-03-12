package org.apache.activemq.broker.openwire;

import static org.apache.activemq.broker.openwire.OpenwireSupport.createConnectionInfo;
import static org.apache.activemq.broker.openwire.OpenwireSupport.createConsumerInfo;
import static org.apache.activemq.broker.openwire.OpenwireSupport.createSessionInfo;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.Connection;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Router;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.TransportFactory;

public class RemoteConsumer extends Connection {

    private final MetricCounter consumerRate = new MetricCounter();

    private MetricAggregator totalConsumerRate;
    private long thinkTime;
    private Destination destination;
    private String selector;
    private URI uri;

    private boolean schedualWait;

    protected final Object inboundMutex = new Object();
    private FlowController<MessageDelivery> inboundController;

    private ActiveMQDestination activemqDestination;
    private ConnectionInfo connectionInfo;
    private SessionInfo sessionInfo;
    private ConsumerInfo consumerInfo;

    private Message lastMessage;
    
    public void start() throws Exception {
        consumerRate.name("Consumer " + name + " Rate");
        totalConsumerRate.add(consumerRate);

        initialize();
        transport = TransportFactory.connect(uri);
        if(transport instanceof DispatchableTransport)
        {
            schedualWait = true;
        }
        super.start();

        if( destination.getDomain().equals( Router.QUEUE_DOMAIN ) ) {
            activemqDestination = new ActiveMQQueue(destination.getName().toString());
        } else {
            activemqDestination = new ActiveMQTopic(destination.getName().toString());
        }
        
        connectionInfo = createConnectionInfo();
        transport.oneway(connectionInfo);
        sessionInfo = createSessionInfo(connectionInfo);
        transport.oneway(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, activemqDestination);
        consumerInfo.setPrefetchSize(1000);
        transport.oneway(consumerInfo);
        
    }
    
    protected void initialize() {
        
        // Setup the input processing..
        Flow flow = new Flow(name, false);
        WindowLimiter<MessageDelivery> limiter = new WindowLimiter<MessageDelivery>(false, flow, inputWindowSize, inputResumeThreshold) {
            protected void sendCredit(int credit) {
                MessageAck ack = OpenwireSupport.createAck(consumerInfo, lastMessage, credit, MessageAck.STANDARD_ACK_TYPE);
                write(ack);
            }
        };
        inboundController = new FlowController<MessageDelivery>(new FlowControllable<MessageDelivery>() {
            public void flowElemAccepted(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
                messageReceived(controller, elem);
            }
            public String toString() {
                return name;
            }
            public IFlowSink<MessageDelivery> getFlowSink() {
                return null;
            }
            public IFlowSource<MessageDelivery> getFlowSource() {
                return null;
            }
        }, flow, limiter, inboundMutex);
    }
    
    public void onCommand(Object command) {
        try {
            if (command.getClass() == WireFormatInfo.class) {
            } else if (command.getClass() == BrokerInfo.class) {
                System.out.println("Consumer "+name+" connected to "+((BrokerInfo)command).getBrokerName());
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
    
    protected void messageReceived(final ISourceController<MessageDelivery> controller, final MessageDelivery elem) {
        if( schedualWait ) {
            if (thinkTime > 0) {
                getDispatcher().schedule(new Runnable(){
                    public void run() {
                        consumerRate.increment();
                        controller.elementDispatched(elem);
                    }
                }, thinkTime, TimeUnit.MILLISECONDS);
                
            }
            else
            {
                consumerRate.increment();
                controller.elementDispatched(elem);
            }

        } else {
            if( thinkTime>0 ) {
                try {
                    Thread.sleep(thinkTime);
                } catch (InterruptedException e) {
                }
            }
            consumerRate.increment();
            controller.elementDispatched(elem);
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public MetricAggregator getTotalConsumerRate() {
        return totalConsumerRate;
    }

    public void setTotalConsumerRate(MetricAggregator totalConsumerRate) {
        this.totalConsumerRate = totalConsumerRate;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public long getThinkTime() {
        return thinkTime;
    }

    public void setThinkTime(long thinkTime) {
        this.thinkTime = thinkTime;
    }

    public MetricCounter getConsumerRate() {
        return consumerRate;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }}
