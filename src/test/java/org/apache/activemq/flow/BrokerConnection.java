/**
 * 
 */
package org.apache.activemq.flow;

import org.apache.activemq.flow.AbstractTestConnection.ReadReadyListener;

class BrokerConnection extends AbstractTestConnection implements MockBroker.DeliveryTarget {
    private final Pipe<Message> pipe;
    private final MockBroker local;

    BrokerConnection(MockBroker local, MockBroker remote, Pipe<Message> pipe) {
        super(local, remote.getName(), null, pipe);
        this.pipe = pipe;
        this.local = local;
    }

    @Override
    protected Message getNextMessage() throws InterruptedException {
        return pipe.read();
    }

    @Override
    protected void addReadReadyListener(final ReadReadyListener listener) {
        pipe.setReadReadyListener(new Pipe.ReadReadyListener<Message>() {
            public void onReadReady(Pipe<Message> pipe) {
                listener.onReadReady();
            }
        });
    }

    public Message pollNextMessage() {
        return pipe.poll();
    }

    @Override
    protected void messageReceived(Message m, ISourceController<Message> controller) {

        m = new Message(m);
        m.hopCount++;

        local.router.route(controller, m);
    }

    @Override
    protected void write(Message m, ISourceController<Message> controller) throws InterruptedException {
        pipe.write(m);
    }

    public IFlowSink<Message> getSink() {
        return output;
    }

    public boolean match(Message message) {
        // Avoid loops:
        if (message.hopCount > 0) {
            return false;
        }

        return true;
    }
}