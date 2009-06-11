package org.apache.activemq.queue.perf;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.TransportFactory;

public class RemoteConsumer extends ClientConnection {

    private final MetricCounter consumerRate = new MetricCounter();

    private MetricAggregator totalConsumerRate;
    private long thinkTime;
    private Destination destination;
    private String selector;

    private boolean schedualWait = true;

    public void start() throws Exception {
        consumerRate.name("Consumer " + name + " Rate");
        totalConsumerRate.add(consumerRate);
        super.start();
        // subscribe:
        write(destination);
    }

    protected void messageReceived(final ISourceController<Message> controller, final Message elem) {
        if (schedualWait) {
            if (thinkTime > 0) {
                getDispatcher().schedule(new Runnable() {

                    public void run() {
                        consumerRate.increment();
                        controller.elementDispatched(elem);
                    }

                }, thinkTime, TimeUnit.MILLISECONDS);

            } else {
                consumerRate.increment();
                controller.elementDispatched(elem);
            }

        } else {
            if (thinkTime > 0) {
                try {
                    Thread.sleep(thinkTime);
                } catch (InterruptedException e) {
                }
            }
            consumerRate.increment();
            controller.elementDispatched(elem);
        }
    }

    public MetricCounter getRate() {
        return consumerRate;
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
}
