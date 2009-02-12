/**
 * 
 */
package org.apache.activemq.flow;

import org.apache.activemq.flow.AbstractTestConnection.ReadReadyListener;
import org.apache.activemq.metric.MetricCounter;

class LocalProducer extends AbstractTestConnection {

    /**
     * 
     */
    private final MockBrokerTest mockBrokerTest;

    MetricCounter producerRate = new MetricCounter();

    private final Destination destination;
    private int msgCounter;
    private String name;
    String property;
    private Message next;
    int msgPriority = 0;
    int priorityMod = 0;
    int producerId;

    public LocalProducer(MockBrokerTest mockBrokerTest, String name, MockBroker broker, Destination destination) {

        super(broker, name, broker.getFlowManager().createFlow(name), null);
        this.mockBrokerTest = mockBrokerTest;
        this.destination = destination;
        this.producerId = this.mockBrokerTest.prodcuerIdGenerator.getAndIncrement();
        
        producerRate.name("Producer " + name + " Rate");
        this.mockBrokerTest.totalProducerRate.add(producerRate);

    }

    /*
     * Gets the next message blocking until space is available for it.
     * (non-Javadoc)
     * 
     * @see com.progress.flow.AbstractTestConnection#getNextMessage()
     */
    public Message getNextMessage() throws InterruptedException {

        Message m = new Message(this.mockBrokerTest.msgIdGenerator.getAndIncrement(), producerId, name + ++msgCounter, flow, destination, msgPriority);
        if (property != null) {
            m.setProperty(property);
        }
        simulateEncodingWork();
        input.getFlowController(m.getFlow()).waitForFlowUnblock();
        return m;
    }

    @Override
    protected void addReadReadyListener(final ReadReadyListener listener) {
        if (next == null) {
            next = new Message(this.mockBrokerTest.msgIdGenerator.getAndIncrement(), producerId, name + ++msgCounter, flow, destination, msgPriority);
            if (property != null) {
                next.setProperty(property);
            }
            simulateEncodingWork();
        }

        if (!input.getFlowController(next.getFlow()).addUnblockListener(new ISinkController.FlowUnblockListener<Message>() {
            public void onFlowUnblocked(ISinkController<Message> controller) {
                listener.onReadReady();
            }
        })) {
            // Return value of false means that the controller didn't
            // register the listener because it was not blocked.
            listener.onReadReady();
        }
    }

    @Override
    public final Message pollNextMessage() {
        if (next == null) {
            int priority = msgPriority;
            if (priorityMod > 0) {
                priority = msgCounter % priorityMod == 0 ? 0 : msgPriority;
            }

            next = new Message(this.mockBrokerTest.msgIdGenerator.getAndIncrement(), producerId, name + ++msgCounter, flow, destination, priority);
            if (property != null) {
                next.setProperty(property);
            }
            simulateEncodingWork();
        }

        if (input.getFlowController(next.getFlow()).isSinkBlocked()) {
            return null;
        }

        Message m = next;
        next = null;
        return m;
    }

    @Override
    public void messageReceived(Message m, ISourceController<Message> controller) {

        broker.router.route(controller, m);
        producerRate.increment();
    }

    @Override
    public void write(Message m, ISourceController<Message> controller) {
        // Noop
    }

}