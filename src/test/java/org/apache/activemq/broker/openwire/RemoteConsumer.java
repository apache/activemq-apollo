package org.apache.activemq.broker.openwire;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.Connection;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
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
    
    public void start() throws Exception {
        consumerRate.name("Consumer " + name + " Rate");
        totalConsumerRate.add(consumerRate);

        transport = TransportFactory.compositeConnect(uri);
        if(transport instanceof DispatchableTransport)
        {
            DispatchableTransport dt = ((DispatchableTransport)transport);
            dt.setName(name + "-client-transport");
            dt.setDispatcher(getDispatcher());
            schedualWait = true;
        }
        transport.setTransportListener(this);
        transport.start();
        
        // Let the remote side know our name.
        transport.oneway(name);
        // Sending the destination acts as the subscribe.
        transport.oneway(destination);
        super.initialize();
    }
    
    protected void initialize() {
        
        // Setup the input processing..
        Flow flow = new Flow(name, false);
        SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(inputWindowSize, inputResumeThreshold);
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
            if (command.getClass() == MessageDelivery.class) {
                MessageDelivery msg = (MessageDelivery) command;
                inboundController.add(msg, null);
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
