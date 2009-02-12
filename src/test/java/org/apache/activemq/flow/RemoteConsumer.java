package org.apache.activemq.flow;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;

public class RemoteConsumer implements TransportListener {

    private final AtomicBoolean stopping = new AtomicBoolean();
    private final MetricCounter consumerRate = new MetricCounter();

    private Transport transport;
    private MockBroker broker;
    private String name;
    private MetricAggregator totalConsumerRate;
    private long thinkTime;
    private Destination destination;
    
    public void start() throws Exception {
        consumerRate.name("Consumer " + name + " Rate");
        totalConsumerRate.add(consumerRate);

        URI uri = broker.transportServer.getConnectURI();
        transport = TransportFactory.connect(uri);
        transport.setTransportListener(this);
        transport.start();
        
        // Sending the destination acts as the subscribe.
        transport.oneway(destination);
    }
    
    public void stop() throws Exception {
        stopping.set(true);
        if( transport!=null ) {
            transport.stop();
            transport=null;
        }
    }

    public void onCommand(Object command) {
        if( command.getClass() == Message.class ) {
            
            if (thinkTime > 0) {
                try {
                    Thread.sleep(thinkTime);
                } catch (InterruptedException e) {
                }
            }
            consumerRate.increment();
            
        } else {
            System.out.println("Unhandled command: "+command);
        }
    }

    public void onException(IOException error) {
        if( !stopping.get() ) {
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

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }


    public MockBroker getBroker() {
        return broker;
    }

    public String getName() {
        return name;
    }

    public MetricAggregator getTotalConsumerRate() {
        return totalConsumerRate;
    }

    public void setTotalConsumerRate(MetricAggregator totalProducerRate) {
        this.totalConsumerRate = totalProducerRate;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }}
