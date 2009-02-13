package org.apache.activemq.flow;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.flow.MockBroker.DeliveryTarget;
import org.apache.activemq.queue.ExclusivePriorityQueue;
import org.apache.activemq.queue.ExclusiveQueue;
import org.apache.activemq.queue.IFlowQueue;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

public class RemoteConnection implements TransportListener, DeliveryTarget {

    protected final Object mutex = new Object();

    protected Transport transport;
    protected MockBroker broker;
    protected IFlowQueue<Message> output;

    protected FlowController<Message> inboundController;
    protected String name;

    private int priorityLevels;

    private final int outputWindowSize = 1000;
    private final int outputResumeThreshold = 500;

    private final int inputWindowSize = 1000;
    private final int inputResumeThreshold = 900;

    private IDispatcher dispatcher;
    private ExecutorService writer;

    private final AtomicBoolean stopping = new AtomicBoolean();

    public void setBroker(MockBroker broker) {
        this.broker = broker;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void start() throws Exception {
        transport.setTransportListener(this);
        transport.start();
    }

    public void stop() throws Exception {
        stopping.set(true);
        writer.shutdown();
        if (transport != null) {
            transport.stop();
        }
    }

    public void onCommand(Object command) {
        try {
            // First command in should be the name of the connection
            if( name==null ) {
                name = (String) command;
                initialize();
            } else if (command.getClass() == Message.class) {
                Message msg = (Message) command;
                // Use the flow controller to send the message on so that we do
                // not overflow
                // the broker.
                while (!inboundController.offer(msg, null)) {
                    inboundController.waitForFlowUnblock();
                }
            } else if (command.getClass() == Destination.class) {
                // This is a subscription request
                Destination destination = (Destination) command;
                broker.subscribe(destination, this);
            }
        } catch (Exception e) {
            onException(e);
        }
    }

    private void initialize() {
        // Setup the input processing..
        SizeLimiter<Message> limiter = new SizeLimiter<Message>(inputWindowSize, inputResumeThreshold);
        Flow flow = new Flow(name + "-inbound", false);
        inboundController = new FlowController<Message>(new FlowControllable<Message>() {
            public void flowElemAccepted(ISourceController<Message> controller, Message elem) {
                broker.router.route(controller, elem);
                inboundController.elementDispatched(elem);
            }

            @Override
            public String toString() {
                return name;
            }
            
            public IFlowSink<Message> getFlowSink() {
                return null;
            }

            public IFlowSource<Message> getFlowSource() {
                return null;
            }
        }, flow, limiter, mutex);

        // Setup output processing
        if (priorityLevels <= 1) {
            limiter = new SizeLimiter<Message>(outputWindowSize, outputResumeThreshold);
            flow = new Flow(name + "-outbound", false);
            ExclusiveQueue<Message> queue = new ExclusiveQueue<Message>(flow, flow.getFlowName(), limiter);
            this.output = queue;
        } else {
            ExclusivePriorityQueue<Message> t = new ExclusivePriorityQueue<Message>(priorityLevels, flow, name + "-outbound", outputWindowSize, outputResumeThreshold);
            t.setPriorityMapper(Message.PRIORITY_MAPPER);
            this.output = t;
        }

        // Use an async thread to drain the output queue.
        // Personally I think it would be better if we polled messages out of the output queue.
        writer = Executors.newSingleThreadExecutor();
        output.setDispatcher(dispatcher);
        output.setDrain(new IFlowDrain<Message>() {
            public void drain(final Message elem, final ISourceController<Message> controller) {
                writer.execute(new Runnable() {
                    public void run() {
                        if (!stopping.get()) {
                            try {
                                transport.oneway(elem);
                                controller.elementDispatched(elem);
                            } catch (IOException e) {
                                onException(e);
                            }
                        }
                    }
                });
            }
        });
    }

    public void onException(IOException error) {
        onException((Exception)error);
    }

    public void onException(Exception error) {
        if (!stopping.get() && !broker.isStopping()) {
            System.out.println("RemoteConnection error: "+error);
            error.printStackTrace();
        }
    }

    public void transportInterupted() {
    }

    public void transportResumed() {
    }

    public String getName() {
        return name;
    }

    public int getPriorityLevels() {
        return priorityLevels;
    }

    public void setPriorityLevels(int priorityLevels) {
        this.priorityLevels = priorityLevels;
    }

    public IDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(IDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public MockBroker getBroker() {
        return broker;
    }

    public int getOutputWindowSize() {
        return outputWindowSize;
    }

    public int getOutputResumeThreshold() {
        return outputResumeThreshold;
    }

    public int getInputWindowSize() {
        return inputWindowSize;
    }

    public int getInputResumeThreshold() {
        return inputResumeThreshold;
    }

    public IFlowSink<Message> getSink() {
        return output;
    }

    public boolean match(Message message) {
        return true;
    }

}
