package org.apache.activemq.flow;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.dispatch.IDispatcher.Dispatchable;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.ISinkController.FlowUnblockListener;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.TransportFactory;

public class RemoteProducer extends RemoteConnection implements Dispatchable, FlowUnblockListener<Message>{

    private static final int FILLER_SIZE = 100;

    private final MetricCounter rate = new MetricCounter();

    private AtomicLong messageIdGenerator;
    private int priority;
    private int priorityMod;
    private int counter;
    private int producerId;
    private Destination destination;
    private String property;
    private MetricAggregator totalProducerRate;
    Message next;
    private DispatchContext dispatchContext;

    private String filler;
    
    public void start() throws Exception {
        
        StringBuilder sb = new StringBuilder(FILLER_SIZE);
        for( int i=0; i < FILLER_SIZE; ++i) {
            sb.append('a'+(i%26));
        }
        filler = sb.toString();
        
        rate.name("Producer " + name + " Rate");
        totalProducerRate.add(rate);

        URI uri = broker.getConnectURI();
        transport = TransportFactory.compositeConnect(uri);
        transport.setTransportListener(this);
        if(transport instanceof DispatchableTransport)
        {
            DispatchableTransport dt = ((DispatchableTransport)transport);
            dt.setName(name);
            dt.setDispatcher(getDispatcher());
        }
        super.setTransport(transport);
       
        super.initialize();
        transport.start();
        // Let the remote side know our name.
        transport.oneway(name);
        dispatchContext = getDispatcher().register(this, name + "-producer");
        dispatchContext.requestDispatch();
    }
    
    public void stop() throws Exception
    {
    	dispatchContext.close();
    	super.stop();
    }

	public void onFlowUnblocked(ISinkController<Message> controller) {
		dispatchContext.requestDispatch();
	}

	public boolean dispatch() {
		while(true)
		{
			
			if(next == null)
			{
	            int priority = this.priority;
	            if (priorityMod > 0) {
	                priority = counter % priorityMod == 0 ? 0 : priority;
	            }
	
	            next = new Message(messageIdGenerator.getAndIncrement(), producerId, createPayload(), null, destination, priority);
	            if (property != null) {
	                next.setProperty(property);
	            }
			}
	        
			//If flow controlled stop until flow control is lifted.
			if(outboundController.isSinkBlocked())
			{
				if(outboundController.addUnblockListener(this))
				{
					return true;
				}
			}
			
	        getSink().add(next, null);
	        rate.increment();
	        next = null;
		}
	}

    private String createPayload() {
        return name + ++counter+filler;
    }
	
	public void setName(String name) {
        this.name = name;
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
}

