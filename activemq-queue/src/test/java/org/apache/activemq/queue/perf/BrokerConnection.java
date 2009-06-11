package org.apache.activemq.queue.perf;

import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.FlowControl;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBuffer;

public class BrokerConnection extends AbstractTestConnection {

    private MockBroker broker;

    public void onCommand(Object command) {
        try {
            // System.out.println("Got Command: " + command);
            // First command in should be the name of the connection
            if (name == null) {
                name = "broker-" + (String) command;
                initialize();
            } else if (command.getClass() == Message.class) {
                Message msg = (Message) command;
                inputQueue.add(msg, null);
            } else if (command.getClass() == DestinationBuffer.class) {
                // This is a subscription request
                Destination destination = (Destination) command;

                broker.subscribe(destination, this);
            } else if (command.getClass() == FlowControlBuffer.class) {
                // This is a subscription request
                FlowControl fc = (FlowControl) command;
                outboundLimiter.onProtocolMessage(fc);
            } else {
                onException(new Exception("Unrecognized command: " + command));
            }
        } catch (Exception e) {
            onException(e);
        }
    }

    protected final void messageReceived(ISourceController<Message> controller, Message elem) {
        broker.router.route(controller, elem);
        controller.elementDispatched(elem);
    }

    public void setBroker(MockBroker broker) {
        this.broker = broker;
    }

    public MockBroker getBroker() {
        return broker;
    }
}
