/**
 * 
 */
package org.apache.activemq.flow;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.flow.AbstractTestConnection.ReadReadyListener;
import org.apache.activemq.flow.MockBrokerTest.DeliveryTarget;
import org.apache.activemq.metric.MetricCounter;

class MockConsumerConnection extends AbstractTestConnection implements MockBrokerTest.DeliveryTarget {

    /**
     * 
     */
    private final MockBrokerTest mockBrokerTest;
    MetricCounter consumerRate = new MetricCounter();
    private final Destination destination;
    String selector;
    private boolean autoRelease = false;

    private long thinkTime = 0;

    public MockConsumerConnection(MockBrokerTest mockBrokerTest, String name, MockBroker broker, Destination destination) {
        super(broker, name, broker.getFlowManager().createFlow(destination.getName()), null);
        this.mockBrokerTest = mockBrokerTest;
        this.destination = destination;
        output.setAutoRelease(autoRelease);
        consumerRate.name("Consumer " + name + " Rate");
        this.mockBrokerTest.totalConsumerRate.add(consumerRate);

    }

    public Destination getDestination() {
        return destination;
    }

    public String getSelector() {
        return selector;
    }

    public void setThinkTime(long time) {
        thinkTime = time;
    }

    @Override
    protected synchronized Message getNextMessage() throws InterruptedException {
        wait();
        return null;
    }

    @Override
    protected void addReadReadyListener(final ReadReadyListener listener) {
        return;
    }

    public Message pollNextMessage() {
        return null;
    }

    @Override
    protected void messageReceived(Message m, ISourceController<Message> controller) {
    }

    @Override
    protected void write(final Message m, final ISourceController<Message> controller) throws InterruptedException {
        if (!m.isSystem()) {
            // /IF we are async don't tie up the calling thread
            // schedule dispatch complete for later.
            if (this.mockBrokerTest.dispatchMode == ASYNC && thinkTime > 0) {
                Runnable acker = new Runnable() {
                    public void run() {
                        if (thinkTime > 0) {
                            try {
                                Thread.sleep(thinkTime);
                            } catch (InterruptedException e) {
                            }
                        }
                        simulateEncodingWork();
                        if (!autoRelease) {
                            controller.elementDispatched(m);
                        }
                        consumerRate.increment();
                    }
                };

                broker.dispatcher.schedule(acker, thinkTime, TimeUnit.MILLISECONDS);
            } else {
                simulateEncodingWork();
                if (!autoRelease) {
                    controller.elementDispatched(m);
                }
                consumerRate.increment();
            }
        }
    }

    public IFlowSink<Message> getSink() {
        return output;
    }

    public boolean match(Message message) {
        if (selector == null)
            return true;
        return message.match(selector);
    }

}