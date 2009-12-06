package org.apache.activemq.queue.perf;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.ISinkController.FlowUnblockListener;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;

public class RemoteProducer extends ClientConnection implements FlowUnblockListener<Message> {

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

    private String filler;
    private int payloadSize = 0;
    IFlowController<Message> outboundController;

    private DispatchQueue dispatchQueue;
    private Runnable dispatchTask;

    public void start() throws Exception {

        if (payloadSize > 0) {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; ++i) {
                sb.append((char) ('a' + (i % 26)));
            }
            filler = sb.toString();
        }

        rate.name("Producer " + name + " Rate");
        totalProducerRate.add(rate);

        super.start();
        outboundController = outputQueue.getFlowController(outboundFlow);
        
        dispatchQueue = getDispatcher().createSerialQueue(name + "-client");
        dispatchTask = new Runnable(){
            public void run() {
                dispatch();
            }
        };
        dispatchQueue.dispatchAsync(dispatchTask);
        
    }

    public void stop() throws Exception {
        dispatchQueue.release();
        super.stop();
    }

    public void onFlowUnblocked(ISinkController<Message> controller) {
        dispatchQueue.dispatchAsync(dispatchTask);
    }

    public void dispatch() {
        while (true) {

            if (next == null) {
                int priority = this.priority;
                if (priorityMod > 0) {
                    priority = counter % priorityMod == 0 ? 0 : priority;
                }

                next = new Message(messageIdGenerator.getAndIncrement(), producerId, createPayload(), null, destination, priority);
                if (property != null) {
                    next.setProperty(property);
                }
            }

            // If flow controlled stop until flow control is lifted.
            if (outboundController.isSinkBlocked()) {
                if (outboundController.addUnblockListener(this)) {
                    return;
                }
            }

            getSink().add(next, null);
            rate.increment();
            next = null;
            dispatchQueue.dispatchAsync(dispatchTask);
        }
    }

    private String createPayload() {
        if (payloadSize >= 0) {
            StringBuilder sb = new StringBuilder(payloadSize);
            sb.append(name);
            sb.append(':');
            sb.append(++counter);
            sb.append(':');
            int length = sb.length();
            if (length <= payloadSize) {
                sb.append(filler.subSequence(0, payloadSize - length));
                return sb.toString();
            } else {
                return sb.substring(0, payloadSize);
            }
        } else {
            return name + ":" + (++counter);
        }
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

    @Override
    protected void messageReceived(ISourceController<Message> controller, Message elem) {
        controller.elementDispatched(elem);
    }
}
