package org.apache.activemq.flow;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;

public class RemoteProducer implements TransportListener, Runnable {

    private final AtomicBoolean stopping = new AtomicBoolean();
    private final MetricCounter rate = new MetricCounter();

    private Transport transport;
    private MockBroker broker;
    private String name;
    private Thread thread;
    private AtomicLong messageIdGenerator;
    private int priority;
    private int priorityMod;
    private int counter;
    private int producerId;
    private Destination destination;
    private String property;
    private MetricAggregator totalProducerRate;
    
    public void start() throws Exception {
        rate.name("Producer " + name + " Rate");
        totalProducerRate.add(rate);

        URI uri = broker.getConnectURI();
        transport = TransportFactory.compositeConnect(uri);
        transport.setTransportListener(this);
        transport.start();
        
        // Let the remote side know our name.
        transport.oneway(name);

        thread = new Thread(this, name);
        thread.start();
    }
    
    public void stop() throws Exception {
        stopping.set(true);
        if( transport!=null ) {
            transport.stop();
        }
        thread.join();
        transport=null;
    }

    public void run() {
        try {
            while( !stopping.get() ) {
                
                int priority = this.priority;
                if (priorityMod > 0) {
                    priority = counter % priorityMod == 0 ? 0 : priority;
                }

                Message next = new Message(messageIdGenerator.getAndIncrement(), producerId, name + ++counter, null, destination, priority);
                if (property != null) {
                    next.setProperty(property);
                }
                
                transport.oneway(next);
                rate.increment();
            }
        } catch (IOException e) {
            onException(e);
        }
    }

    public void onCommand(Object command) {
        System.out.println("Unhandled command: "+command);
    }

    public void onException(IOException error) {
        if( !stopping.get() ) {
            System.out.println("RemoteProducer error: "+error);
            error.printStackTrace();
        }
    }

    public void transportInterupted() {
    }
    public void transportResumed() {
    }

    public void setName(String name) {
        this.name = name;
    }
    public void setBroker(MockBroker broker) {
        this.broker = broker;
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

    public MockBroker getBroker() {
        return broker;
    }

    public String getName() {
        return name;
    }

    public MetricAggregator getTotalProducerRate() {
        return totalProducerRate;
    }

    public void setTotalProducerRate(MetricAggregator totalProducerRate) {
        this.totalProducerRate = totalProducerRate;
    }

    public MetricCounter getRate() {
        return rate;
    }}
