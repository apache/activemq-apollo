package org.apache.activemq.flow;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.FlowControl;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBean;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBuffer;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.flow.MockBroker.DeliveryTarget;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

public class RemoteConnection implements TransportListener, DeliveryTarget {

    protected Transport transport;
    protected MockBroker broker;

    protected final Object inboundMutex = new Object();
    protected IFlowController<Message> inboundController;

    protected SingleFlowRelay<Message> outputQueue;
    protected IFlowController<Message> outboundController;
    protected ProtocolLimiter<Message> outboundLimiter;
    protected Flow ouboundFlow;

    protected String name;

    private int priorityLevels;

    private final int outputWindowSize = 1000;
    private final int outputResumeThreshold = 900;

    private final int inputWindowSize = 1000;
    private final int inputResumeThreshold = 900;

    private IDispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean();
    protected Flow outputFlow;
    protected boolean blockingTransport = false;
    ExecutorService blockingWriter;

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
        if (transport != null) {
            transport.stop();
        }
        if (blockingWriter != null) {
            blockingWriter.shutdown();
        }
    }

    public void onCommand(Object command) {
        try {
            // System.out.println("Got Command: " + command);
            // First command in should be the name of the connection
            if (name == null) {
                name = (String) command;
                initialize();
            } else if (command.getClass() == Message.class) {
                Message msg = (Message) command;
                inboundController.add(msg, null);
            } else if (command.getClass() == DestinationBuffer.class) {
                // This is a subscription request
                Destination destination = (Destination) command;

                broker.subscribe(destination, this);
            } else if (command.getClass() == FlowControlBuffer.class) {
                // This is a subscription request
                FlowControl fc = (FlowControl) command;
                synchronized (outputQueue) {
                    outboundLimiter.onProtocolMessage(fc);
                }
            } else {
                onException(new Exception("Unrecognized command: " + command));
            }
        } catch (Exception e) {
            onException(e);
        }
    }

    protected void initialize() {
        // Setup the input processing..
        Flow flow = new Flow(name, false);
        WindowLimiter<Message> limiter = new WindowLimiter<Message>(false, flow, inputWindowSize, inputResumeThreshold);

        inboundController = new FlowController<Message>(new FlowControllable<Message>() {
            public void flowElemAccepted(ISourceController<Message> controller, Message elem) {
                messageReceived(controller, elem);
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
        }, flow, limiter, inboundMutex);

        ouboundFlow = new Flow(name, false);
        outboundLimiter = new WindowLimiter<Message>(true, ouboundFlow, outputWindowSize, outputResumeThreshold);
        outputQueue = new SingleFlowRelay<Message>(ouboundFlow, name + "-outbound", outboundLimiter);
        outboundController = outputQueue.getFlowController(ouboundFlow);

        if (transport instanceof DispatchableTransport) {
            outputQueue.setDrain(new IFlowDrain<Message>() {

                public void drain(Message message, ISourceController<Message> controller) {
                    write(message);
                }
            });

        } else {
            blockingTransport = true;
            blockingWriter = Executors.newSingleThreadExecutor();
            outputQueue.setDrain(new IFlowDrain<Message>() {
                public void drain(final Message message, ISourceController<Message> controller) {
                    write(message);
                };
            });
            /*
             * // Setup output processing final Executor writer =
             * Executors.newSingleThreadExecutor(); FlowControllable<Message>
             * controllable = new FlowControllable<Message>() { public void
             * flowElemAccepted( final ISourceController<Message> controller,
             * final Message elem) { writer.execute(new Runnable() { public void
             * run() { if (!stopping.get()) { try { transport.oneway(elem);
             * controller.elementDispatched(elem); } catch (IOException e) {
             * onException(e); } } } }); }
             * 
             * public IFlowSink<Message> getFlowSink() { return null; }
             * 
             * public IFlowSource<Message> getFlowSource() { return null; } };
             * 
             * if (priorityLevels <= 1) { outboundController = new
             * FlowController<Message>(controllable, flow, limiter,
             * outboundMutex); } else { PrioritySizeLimiter<Message> pl = new
             * PrioritySizeLimiter<Message>( outputWindowSize,
             * outputResumeThreshold, priorityLevels);
             * pl.setPriorityMapper(Message.PRIORITY_MAPPER); outboundController
             * = new PriorityFlowController<Message>( controllable, flow, pl,
             * outboundMutex); }
             */
        }
        // outputQueue.setDispatcher(dispatcher);

    }

    private final void write(final Object o) {
        synchronized (outputQueue) {
            if (!blockingTransport) {
                try {
                    transport.oneway(o);
                } catch (IOException e) {
                    onException(e);
                }
            } else {
                blockingWriter.execute(new Runnable() {
                    public void run() {
                        if (!stopping.get()) {
                            try {
                                transport.oneway(o);
                            } catch (IOException e) {
                                onException(e);
                            }
                        }
                    }
                });
            }
        }
    }

    protected void messageReceived(ISourceController<Message> controller, Message elem) {
        broker.router.route(controller, elem);
        inboundController.elementDispatched(elem);
    }

    public void onException(IOException error) {
        onException((Exception) error);
    }

    public void onException(Exception error) {
        if (!stopping.get() && !broker.isStopping()) {
            System.out.println("RemoteConnection error: " + error);
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
        if (transport instanceof DispatchableTransport) {
            DispatchableTransport dt = ((DispatchableTransport) transport);
            if (name != null) {
                dt.setName(name);
            }
            dt.setDispatcher(getDispatcher());
        }
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
        return outputQueue;
    }

    public boolean match(Message message) {
        return true;
    }

    private interface ProtocolLimiter<E> extends IFlowLimiter<E> {
        public void onProtocolMessage(FlowControl m);
    }

    private class WindowLimiter<E> extends SizeLimiter<E> implements ProtocolLimiter<E> {
        final Flow flow;
        final boolean clientMode;
        private int available;

        public WindowLimiter(boolean clientMode, Flow flow, int capacity, int resumeThreshold) {
            super(capacity, resumeThreshold);
            this.clientMode = clientMode;
            this.flow = flow;
        }

        public void reserve(E elem) {
            super.reserve(elem);
            if (!clientMode) {
                // System.out.println(RemoteConnection.this.name + " Reserved "
                // + this);
            }
        }

        public void releaseReserved(E elem) {
            super.reserve(elem);
            if (!clientMode) {
                // System.out.println(RemoteConnection.this.name +
                // " Released Reserved " + this);
            }
        }

        protected void remove(int size) {
            super.remove(size);
            if (!clientMode) {
                available += size;
                if (available >= capacity - resumeThreshold) {
                    FlowControlBean fc = new FlowControlBean();
                    fc.setCredit(available);
                    write(fc.freeze());
                    // System.out.println(RemoteConnection.this.name +
                    // " Send Release " + available + this);
                    available = 0;
                }
            }
        }

        public void onProtocolMessage(FlowControl m) {
            remove(m.getCredit());
        }

        public int getElementSize(Message m) {
            return m.getFlowLimiterSize();
        }
    }

}
