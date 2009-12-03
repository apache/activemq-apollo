package org.apache.activemq.queue.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.internal.advanced.IDispatcher;
import org.apache.activemq.flow.AbstractLimiter;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.Commands.FlowControl;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBean;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBuffer;
import org.apache.activemq.queue.AbstractFlowRelay;
import org.apache.activemq.queue.ExclusiveQueue;
import org.apache.activemq.queue.IFlowQueue;
import org.apache.activemq.queue.IPollableFlowSource;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.queue.IPollableFlowSource.FlowReadyListener;
import org.apache.activemq.queue.perf.MockBroker.DeliveryTarget;
import org.apache.activemq.transport.AsyncTransport;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.AsyncTransport.CommandSource;

public abstract class AbstractTestConnection implements TransportListener, DeliveryTarget {

    private static final boolean USE_RATE_BASED_LIMITER = false;
    protected static final boolean USE_OUTPUT_QUEUE = true;
    private static final boolean USE_ASYNC_COMMAND_QUEUE = true || USE_OUTPUT_QUEUE;

    protected static final RateBasedLimiterCollector RATE_BASED_LIMITER_COLLECTOR = new RateBasedLimiterCollector();
    protected static final AtomicBoolean inShutdown = new AtomicBoolean(true);

    protected Transport transport;
    protected AsyncTransport asyncTransport;
    protected AsyncCommandQueue asyncCommandQueue;

    protected boolean useInputQueue = false;
    protected AbstractFlowRelay<Message> inputQueue;

    protected Flow outboundFlow;
    protected AbstractFlowRelay<Message> outputQueue;
    protected IPollableFlowSource<Message> outputSource;
    protected ProtocolLimiter<Message> outboundLimiter;

    protected String name;

    private int priorityLevels;

    private final int outputWindowSize = 1000;
    private final int outputResumeThreshold = 900;

    private final int inputWindowSize = 1000;
    private final int inputResumeThreshold = 500;

    private IDispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    protected boolean blockingTransport = false;
    ExecutorService blockingWriter;

    public static void setInShutdown(boolean val, IDispatcher dispatcher) {
        if (val != inShutdown.getAndSet(val)) {
            if (val) {
                if (USE_RATE_BASED_LIMITER) {
                    RATE_BASED_LIMITER_COLLECTOR.shutdown();
                }
            } else {
                if (USE_RATE_BASED_LIMITER) {
                    RATE_BASED_LIMITER_COLLECTOR.setDispatcher(dispatcher);
                }
            }
        }
    }

    public void setUseInputQueue(boolean useInputQueue) {
        this.useInputQueue = useInputQueue;
    }

    public boolean isUseInputQueue() {
        return useInputQueue;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void start() throws Exception {
        transport.setTransportListener(this);
        if (transport instanceof DispatchableTransport) {
            DispatchableTransport dt = ((DispatchableTransport) transport);
            if (name != null) {
                dt.setName(name + "-transport");
            }
            dt.setDispatcher(getDispatcher());
            dt.setDispatchPriority(dispatcher.getDispatchPriorities() - 1);
        }
        transport.start();
    }

    public void stop() throws Exception {
        stopping.set(true);
        if (transport != null) {
            transport.stop();
        }
        if (outboundLimiter != null) {
            outboundLimiter.shutdown();
        }
        if (blockingWriter != null) {
            blockingWriter.shutdown();
        }
    }

    public void onCommand(Object command) {
        try {
            if (command.getClass() == Message.class) {
                Message msg = (Message) command;
                inputQueue.add(msg, null);
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

    protected void initialize() {
        // Setup the input processing..
        Flow flow = new Flow(name, false);
        ProtocolLimiter<Message> limiter = createProtocolLimiter(false, flow, inputWindowSize, inputResumeThreshold);

        if (!useInputQueue) {
            inputQueue = new SingleFlowRelay<Message>(flow, name + "-inbound", limiter);
        } else {
            ExclusiveQueue<Message> queue = new ExclusiveQueue<Message>(flow, name + "-inbound", limiter);
            queue.setDispatchPriority(0);
            queue.setDispatcher(dispatcher);
            queue.getFlowController(flow).useOverFlowQueue(false);
            inputQueue = queue;
        }
        inputQueue.setFlowExecutor(dispatcher.createPriorityExecutor(dispatcher.getDispatchPriorities() - 1));
        inputQueue.setDrain(new QueueDispatchTarget<Message>() {

            public void drain(Message message, ISourceController<Message> controller) {
                messageReceived(controller, message);
            }
        });

        if (USE_ASYNC_COMMAND_QUEUE) {
            asyncTransport = transport.narrow(AsyncTransport.class);
            if (asyncTransport != null) {
                asyncCommandQueue = new AsyncCommandQueue();
                asyncTransport.setCommandSource(asyncCommandQueue);
            }
        }

        outboundFlow = new Flow(name, false);
        outboundLimiter = createProtocolLimiter(true, outboundFlow, outputWindowSize, outputResumeThreshold);

        if (transport.narrow(DispatchableTransport.class) == null) {
            blockingTransport = true;
            blockingWriter = Executors.newSingleThreadExecutor();
        }

        if (!USE_OUTPUT_QUEUE || asyncTransport == null || blockingTransport) {
            outputQueue = new SingleFlowRelay<Message>(outboundFlow, name + "-outbound", outboundLimiter);
            outputQueue.setDrain(new QueueDispatchTarget<Message>() {
                public void drain(final Message message, ISourceController<Message> controller) {
                    write(message);
                };
            });
        } else {
            ExclusiveQueue<Message> queue = new ExclusiveQueue<Message>(outboundFlow, name + "-outbound", outboundLimiter);
            outputQueue = queue;
            outputSource = queue;
            queue.addFlowReadyListener(asyncCommandQueue);
        }
        // Set the executor to be used by the queue's flow controllers:
        outputQueue.setFlowExecutor(dispatcher.createPriorityExecutor(dispatcher.getDispatchPriorities() - 1));

        limiter.start();
        outboundLimiter.start();

    }

    protected final ProtocolLimiter<Message> createProtocolLimiter(boolean clientMode, Flow flow, int capacity, int resumeThreshold) {
        if (USE_RATE_BASED_LIMITER) {
            return new RateBasedLimiter(clientMode, flow);
        } else {
            return new WindowLimiter<Message>(clientMode, flow, capacity, resumeThreshold);
        }
    }

    protected final void write(final Object o) {

        if (asyncTransport != null) {
            asyncCommandQueue.addCommand(o);
            return;
        }
        synchronized (outputQueue) {
            if (!blockingTransport) {
                try {
                    transport.oneway(o);
                } catch (IOException e) {
                    onException(e);
                }
            } else {
                try {
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
                } catch (RejectedExecutionException re) {
                    // Must be shutting down.
                }
            }
        }
    }

    protected abstract void messageReceived(ISourceController<Message> controller, Message elem);

    public void onException(IOException error) {
        onException((Exception) error);
    }

    public void onException(Exception error) {
        if (!(stopping.get() || inShutdown.get())) {
            System.out.println(transport.toString() + " - Connection error: " + error);
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

    public final boolean hasSelector() {
        return false;
    }

    public boolean match(Message message) {
        return true;
    }

    private void sendFlowControl(int available) {
        if (asyncTransport == null) {
            FlowControlBean fc = new FlowControlBean();
            fc.setCredit(available);
            write(fc.freeze());
        } else {
            asyncCommandQueue.addFlowControlReady(available);
        }
    }

    protected interface ProtocolLimiter<E> extends IFlowLimiter<E> {
        public void onProtocolMessage(FlowControl m);

        public void start();

        public void shutdown();
    }

    private class AsyncCommandQueue implements CommandSource, FlowReadyListener<Message> {
        private final LinkedList<Object> outputCommandQueue = new LinkedList<Object>();
        private boolean needsNotify = true;
        private int available = 0;
        private boolean delayable = false;
        private boolean registered = false;

        public Object pollNextCommand() {
            synchronized (outputQueue) {
                delayable = false;
                if (available > 0) {
                    FlowControlBean fc = new FlowControlBean();
                    fc.setCredit(available);
                    available = 0;
                    return fc.freeze();
                }

                delayable = !outboundLimiter.getThrottled();
                Object command = null;

                if (!outputCommandQueue.isEmpty()) {
                    command = outputCommandQueue.removeFirst();
                    if (command instanceof Message) {
                        delayable = !outboundLimiter.getThrottled();
                    } else {
                        delayable = false;
                    }
                    return command;
                }

                if (outputSource != null) {
                    command = outputSource.poll();
                    if (command != null) {
                        return command;
                    } else if (!registered) {
                        registered = true;
                        outputSource.addFlowReadyListener(this);
                    }
                }

                needsNotify = true;
                return null;
            }
        }

        public boolean delayable() {
            return delayable;
        }

        public void addCommand(Object object) {
            synchronized (outputQueue) {
                outputCommandQueue.add(object);
                notifyTransport();
            }
        }

        public void addFlowControlReady(int available) {
            synchronized (outputQueue) {
                this.available += available;
                notifyTransport();
            }
        }

        public final void notifyTransport() {
            if (true) {
                asyncTransport.onCommandReady();
            } else if (needsNotify) {
                needsNotify = false;
                asyncTransport.onCommandReady();
            }
        }

        public void onFlowReady(IPollableFlowSource<Message> source) {
            synchronized (outputQueue) {
                registered = false;
                notifyTransport();
            }
        }

    }

    protected class WindowLimiter<E> extends SizeLimiter<E> implements ProtocolLimiter<E> {
        final Flow flow;
        final boolean clientMode;
        private int available;

        public WindowLimiter(boolean clientMode, Flow flow, int capacity, int resumeThreshold) {
            super(capacity, resumeThreshold);
            this.clientMode = clientMode;
            this.flow = flow;
        }

        @Override
        public void remove(int count, long size) {
            super.remove(count, size);
            if (!clientMode) {
                available += size;
                if (available >= capacity - resumeThreshold) {
                    sendFlowControl(available);
                    // System.out.println(RemoteConnection.this.name +
                    // " Send Release " + available + this);
                    available = 0;
                }
            }
        }

        public void onProtocolMessage(FlowControl m) {
            synchronized (outputQueue) {
                remove(1, m.getCredit());
            }
        }

        public int getElementSize(Message m) {
            return m.getFlowLimiterSize();
        }

        public void shutdown() {
            // Noop
        }

        public void start() {
            // Noop
        }
    }

    protected static class RateBasedLimiterCollector implements Runnable {

        private IDispatcher dispatcher;
        private int samplingPeriod = 50;
        private boolean scheduled = false;
        private HashSet<RateBasedLimiter> limiters = new HashSet<RateBasedLimiter>();

        public synchronized void setDispatcher(IDispatcher d) {
            if (d != dispatcher) {
                scheduled = false;
                dispatcher = d;
            }

            dispatcher = d;
            scheduleNext();
        }

        public synchronized void shutdown() {
            limiters.clear();
        }

        public synchronized void addLimiter(RateBasedLimiter limiter) {
            if (limiters.isEmpty()) {
                limiters.add(limiter);
                scheduleNext();
            } else {
                limiters.add(limiter);
            }
        }

        public synchronized void removeLimiter(RateBasedLimiter limiter) {
            limiters.remove(limiter);
        }

        public void run() {

            ArrayList<RateBasedLimiter> toCollect = null;

            synchronized (this) {
                if (!limiters.isEmpty()) {
                    toCollect = new ArrayList<RateBasedLimiter>(limiters.size());
                    toCollect.addAll(limiters);
                }
            }

            if (toCollect != null) {
                for (RateBasedLimiter limiter : toCollect) {
                    limiter.update();
                }
            }

            synchronized (this) {
                scheduled = false;
                scheduleNext();
            }
        }

        private void scheduleNext() {
            synchronized (this) {
                if (dispatcher == null) {
                    return;
                }
                if (!scheduled && !limiters.isEmpty()) {
                    scheduled = true;
                    dispatcher.schedule(this, samplingPeriod, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    protected class RateBasedLimiter extends AbstractLimiter<Message> implements ProtocolLimiter<Message> {
        final Flow flow;
        final boolean clientMode;
        private boolean throttled = false;

        private long counter = 0;
        private int limit = 0;

        private float targetRate = 200000;
        private final int quantum = 10000;
        //private final float lambda = .0001f;
        boolean increase = true;

        private static final boolean DEBUG = false;

        public RateBasedLimiter(boolean clientMode, Flow flow) {
            this.clientMode = clientMode;
            this.flow = flow;
            limit = (int) (targetRate * RATE_BASED_LIMITER_COLLECTOR.samplingPeriod * .0001f);
        }

        public void start() {
            if (clientMode) {
                RATE_BASED_LIMITER_COLLECTOR.addLimiter(this);
            }
        }

        public void shutdown() {
            if (clientMode) {
                RATE_BASED_LIMITER_COLLECTOR.removeLimiter(this);
            }
        }

        public void onProtocolMessage(FlowControl m) {
            synchronized (outputQueue) {
                if (m.getCredit() == 1) {
                    abate();
                } else {
                    congest();
                }
            }
        }

        private void congest() {
            targetRate -= quantum;
            increase = false;
            throttled = true;
            // if (DEBUG)
            debug("congest");
        }

        private void abate() {

            /*
             * if (throttled) { throttled = false;
             * super.notifyUnThrottleListeners(); }
             */

            // if (DEBUG)
            debug("abate");

        }

        public void update() {
            synchronized (outputQueue) {

                if (DEBUG)
                    debug("Update");
                counter = 0;

                if (increase) {
                    targetRate += quantum;
                } else {
                    increase = true;
                }

                limit = (int) (targetRate * RATE_BASED_LIMITER_COLLECTOR.samplingPeriod * .0001f);

                if (throttled) {
                    throttled = false;
                    notifyUnThrottleListeners();
                }
            }
        }

        public boolean add(Message elem) {
            if (clientMode) {
                counter += elem.getFlowLimiterSize();
                if (counter >= limit) {
                    throttled = true;
                }
                if (DEBUG)
                    debug("Add");

            }
            return throttled;
        }

        public boolean canAdd(Message elem) {
            return !throttled;
        }

        public boolean getThrottled() {
            return throttled;
        }

        public void remove(Message elem) {
            // Noop
        }

        public void releaseReserved() {
            if (!clientMode) {
                // Send abate
                if (throttled) {
                    notifyUnThrottleListeners();
                    throttled = false;
                    sendFlowControl(1);
                }
            }
        }

        public void reserve(Message elem) {
            if (!clientMode) {
                // Send congest:
                if (!throttled) {
                    throttled = true;
                    sendFlowControl(0);
                }
            }
        }

        private void debug(String str) {
            System.out.println(AbstractTestConnection.this.name + " " + str + " count/limit/throttled: " + counter + "/" + limit + "/" + throttled);
        }
    }

}
