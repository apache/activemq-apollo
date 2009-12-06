package org.apache.activemq.broker;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.apollo.Connection;
import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISinkController.FlowUnblockListener;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.transport.TransportFactory;

import static org.apache.activemq.dispatch.DispatchOption.*;

abstract public class RemoteProducer extends Connection implements FlowUnblockListener<MessageDelivery> {

    protected final MetricCounter rate = new MetricCounter();

    protected AtomicLong messageIdGenerator;
    protected int priority;
    protected boolean persistentDelivery;
    protected int priorityMod;
    protected int counter;
    protected int producerId;
    protected Destination destination;
    protected String property;
    protected MetricAggregator totalProducerRate;
    protected MessageDelivery next;
    protected DispatchQueue dispatchQueue;
    protected Runnable dispatchTask;
    protected String filler;
    protected int payloadSize = 20;
    protected URI uri;

    protected IFlowController<MessageDelivery> outboundController;
    protected IFlowSink<MessageDelivery> outboundQueue;

    
    public void start() throws Exception {
        
        if( payloadSize>0 ) {
            StringBuilder sb = new StringBuilder(payloadSize);
            for( int i=0; i < payloadSize; ++i) {
                sb.append((char)('a'+(i%26)));
            }
            filler = sb.toString();
        }
        
        rate.name("Producer " + name + " Rate");
        totalProducerRate.add(rate);


        transport = TransportFactory.compositeConnect(uri);
        initialize();
        super.start();
        
        setupProducer();
        
        dispatchQueue = getDispatcher().createSerialQueue(name + "-client", STICK_TO_CALLER_THREAD);
        dispatchTask = new Runnable(){
            public void run() {
                dispatch();
            }
        };
        dispatchQueue.dispatchAsync(dispatchTask);

    }
    
    public void dispatch() {
        while(true)
        {
            
            if(next == null)
            {
                createNextMessage();
            }
            
            //If flow controlled stop until flow control is lifted.
            if(outboundController.isSinkBlocked())
            {
                if(outboundController.addUnblockListener(this))
                {
                    return;
                }
            }
            
            outboundQueue.add(next, null);
            rate.increment();
            next = null;
        }
    }

    abstract protected void setupProducer() throws Exception;
    
    abstract protected void createNextMessage();

    public void stop() throws Exception
    {
    	dispatchQueue.release();
    	super.stop();
    }
    
	public void onFlowUnblocked(ISinkController<MessageDelivery> controller) {
        dispatchQueue.dispatchAsync(dispatchTask);
	}

    protected String createPayload() {
        if( payloadSize>=0 ) {
            StringBuilder sb = new StringBuilder(payloadSize);
            sb.append(name);
            sb.append(':');
            sb.append(++counter);
            sb.append(':');
            int length = sb.length();
            if( length <= payloadSize ) {
                sb.append(filler.subSequence(0, payloadSize-length));
                return sb.toString();
            } else {
               return sb.substring(0, payloadSize); 
            }
        } else {
            return name+":"+(++counter);
        }
    }
    
    public boolean isPersistentDelivery() {
        return persistentDelivery;
    }

    public void setPersistentDelivery(boolean persistentDelivery) {
        this.persistentDelivery = persistentDelivery;
    }
	
    public AtomicLong getMessageIdGenerator() {
        return messageIdGenerator;
    }

    public void setMessageIdGenerator(AtomicLong msgIdGenerator) {
        this.messageIdGenerator = msgIdGenerator;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int msgPriority) {
        this.priority = msgPriority;
    }

    public int getPriorityMod() {
        return priorityMod;
    }

    public void setPriorityMod(int priorityMod) {
        this.priorityMod = priorityMod;
    }

    public int getProducerId() {
        return producerId;
    }

    public void setProducerId(int producerId) {
        this.producerId = producerId;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public MetricAggregator getTotalProducerRate() {
        return totalProducerRate;
    }

    public void setTotalProducerRate(MetricAggregator totalProducerRate) {
        this.totalProducerRate = totalProducerRate;
    }

    public MetricCounter getRate() {
        return rate;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public void setPayloadSize(int messageSize) {
        this.payloadSize = messageSize;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }
}

