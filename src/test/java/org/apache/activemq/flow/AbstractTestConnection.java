/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.flow;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.Service;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.dispatch.IDispatcher.Dispatchable;
import org.apache.activemq.flow.AbstractLimitedFlowSource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowDrain;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.IFlowResource.FlowLifeCycleListener;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.queue.ExclusivePriorityQueue;
import org.apache.activemq.queue.IAsynchronousFlowSource;
import org.apache.activemq.queue.IBlockingFlowSource;
import org.apache.activemq.queue.IFlowQueue;
import org.apache.activemq.queue.IPollableFlowSource;
import org.apache.activemq.queue.SingleFlowPriorityQueue;

public abstract class AbstractTestConnection implements Service {
    
    protected final IFlowQueue<Message> output;
    protected final NetworkSource input;
    protected final MockBroker broker;
    protected final String name;
    protected final Flow flow;

    private AtomicBoolean running = new AtomicBoolean();
    private Thread listener;
    private Thread sender;

    private final int outputQueueSize = 1000;
    private final int resumeThreshold = 500;

    private final int inputWindowSize = 1000;
    private final int inputResumeThreshold = 900;

    public static final int BLOCKING = 0;
    public static final int POLLING = 1;
    public static final int ASYNC = 2;

    AbstractTestConnection(MockBroker broker, String name, Flow flow, Pipe<Message> p) {
        this.name = name;
        this.broker = broker;
        this.flow = flow;

        // Set up an input source:
        this.input = new NetworkSource(flow, name + "-INPUT", inputWindowSize, inputResumeThreshold);

        // Setup output queue:
        if (MockBrokerTest.PRIORITY_LEVELS <= 1) {
            this.output = TestFlowManager.createFlowQueue(flow, name + "-OUTPUT", outputQueueSize, resumeThreshold);
        } else {
            ExclusivePriorityQueue<Message> t = new ExclusivePriorityQueue<Message>(MockBrokerTest.PRIORITY_LEVELS, flow, name + "-OUTPUT", outputQueueSize, resumeThreshold);
            t.setPriorityMapper(Message.PRIORITY_MAPPER);
            this.output = t;
        }

        output.setDrain(new IFlowDrain<Message>() {
            public void drain(Message m, ISourceController<Message> controller) {
                try {
                    write(m, controller);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    Thread.currentThread().interrupt();
                }
            }
        });

        // We must watch the output for open and close of flows and communicate
        // it
        // to the peer.
        output.addFlowLifeCycleListener(new FlowLifeCycleListener() {
            public void onFlowClosed(IFlowResource resource, Flow flow) {
                try {
                    write(new FlowClose(flow), null);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            public void onFlowOpened(IFlowResource resource, Flow flow) {
                try {
                    // Set the limiter to a WindowedLimiter capable of handling
                    // flow control messages from the peer:
                    output.getFlowController(flow).setLimiter(new WindowLimiter<Message>(true, flow, outputQueueSize, resumeThreshold));
                    // Tell the other side that we've open a flow.
                    write(new FlowOpen(flow), null);

                    FlowController<Message> controller = output.getFlowController(flow);
                    if (controller != null) {
                        controller.setLimiter(new WindowLimiter<Message>(true, flow, outputQueueSize, resumeThreshold));
                        // Tell the other side that we've open a flow.
                        write(new FlowOpen(flow), null);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        });

        output.setDispatchPriority(0);

    }

    public final void simulateEncodingWork() {
        if (MockBrokerTest.IO_WORK_AMOUNT > 1) {
            fib(MockBrokerTest.IO_WORK_AMOUNT);
        }
    }

    public final void fib(int n) {
        if (n > 1) {
            fib(n - 1);
            fib(n - 2);
        }
    }

    private interface ProtocolLimiter {
        public void onProtocolMessage(Message m);
    }

    private class WindowLimiter<E> extends SizeLimiter<E> implements ProtocolLimiter {
        final Flow flow;
        final boolean clientMode;
        private int available;

        public WindowLimiter(boolean clientMode, Flow flow, int capacity, int resumeThreshold) {
            super(capacity, resumeThreshold);
            this.clientMode = clientMode;
            this.flow = flow;
        }

        protected void remove(int size) {
            super.remove(size);
            if (!clientMode) {
                available += size;
                if (available > capacity - resumeThreshold) {
                    try {
                        write(new FlowMessage(flow, available, 0), null);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    available = 0;
                }
            }
        }

        public void onProtocolMessage(Message m) {
            if (m.type() == Message.TYPE_FLOW_CONTROL) {
                FlowMessage fm = (FlowMessage) m;
                super.remove(fm.size);
            }
        }

        public int getElementSize(Message m) {
            return m.getFlowLimiterSize();
        }
    }

    public final void start() throws Exception {
        running.set(true);
        if (MockBrokerTest.DISPATCH_MODE == BLOCKING) {
            listener = new Thread(new Runnable() {
                public void run() {
                    try {
                        while (true) {
                            input.blockingDispatch();
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, name + "-Listener");
            listener.start();
            sender = new Thread(new Runnable() {
                public void run() {
                    try {
                        while (true) {
                            output.blockingDispatch();
                        }
                    } catch (InterruptedException e) {
                    }
                }
            }, name + "-Sender");
            sender.start();
        } else {
            output.setDispatcher(broker.getDispatcher());
            input.setDispatcher(broker.getDispatcher());
            return;
        }

    }

    public final void stop() throws Exception {
        running.set(false);
        if (MockBrokerTest.DISPATCH_MODE == BLOCKING) {
            listener.interrupt();
            listener.join();
            sender.interrupt();
            sender.join();
        }
    }

    protected abstract Message getNextMessage() throws InterruptedException;

    protected abstract Message pollNextMessage();

    protected abstract void addReadReadyListener(ReadReadyListener listener);

    /**
     * Must be implemented to write a message.
     * 
     * @param m
     */
    protected abstract void write(Message m, ISourceController<Message> controller) throws InterruptedException;

    /**
     * Handles a received message.
     */
    protected abstract void messageReceived(Message m, ISourceController<Message> controller);

    /**
     * Simulates a network source of messages.
     * 
     * @param <E>
     */
    protected class NetworkSource extends AbstractLimitedFlowSource<Message> implements IBlockingFlowSource<Message>, IAsynchronousFlowSource<Message>, IPollableFlowSource<Message>,
            FlowControllable<Message> {
        private final FlowController<Message> flowController;
        private final Flow flow;
        private final IFlowQueue<Message> inputQueue;
        private DispatchContext dispatchContext;

        public NetworkSource(Flow flow, String name, int capacity, int resumeThreshold) {
            super(name);
            if (flow == null) {
                if (MockBrokerTest.USE_INPUT_QUEUES) {
                    inputQueue = TestFlowManager.createFlowQueue(flow, name, capacity, resumeThreshold);
                } else {
                    inputQueue = null;
                }
                flowController = null;
            } else {
                if (MockBrokerTest.USE_INPUT_QUEUES) {
                    if (MockBrokerTest.PRIORITY_LEVELS <= 1) {
                        inputQueue = TestFlowManager.createFlowQueue(flow, name, capacity, resumeThreshold);
                    } else {
                        SingleFlowPriorityQueue<Message> t = new SingleFlowPriorityQueue<Message>(flow, name, new SizeLimiter<Message>(capacity, resumeThreshold));
                        t.setPriorityMapper(Message.PRIORITY_MAPPER);
                        inputQueue = t;
                    }
                    flowController = inputQueue.getFlowController(flow);
                    // Allow overflow we should be limited by protocol:
                    flowController.useOverFlowQueue(false);
                    flowController.setLimiter(new SizeLimiter<Message>(capacity, resumeThreshold));
                    super.onFlowOpened(flowController);
                } else {
                    inputQueue = null;
                    SizeLimiter<Message> limiter = new SizeLimiter<Message>(capacity, resumeThreshold);
                    flowController = new FlowController<Message>(this, flow, limiter, this);
                    // Allow overflow we should be limited by protocol:
                    flowController.useOverFlowQueue(false);
                    super.onFlowOpened(flowController);
                }
            }
            this.flow = flow;
        }

        public synchronized void setDispatcher(final IDispatcher dispatcher) {

            if (inputQueue != null) {
                inputQueue.setDrain(new IFlowDrain<Message>() {

                    public void drain(Message elem, ISourceController<Message> controller) {
                        messageReceived(elem, controller);
                        controller.elementDispatched(elem);
                    }

                });
                inputQueue.setDispatcher(dispatcher);
                inputQueue.setDispatchPriority(0);
            }

            dispatchContext = dispatcher.register(new Dispatchable() {
                public boolean dispatch() {
                    if (!pollingDispatch()) {
                        addReadReadyListener(new ReadReadyListener() {
                            public void onReadReady() {
                                if (running.get()) {
                                    dispatchContext.requestDispatch();
                                }
                            }
                        });
                        return true;
                    }
                    return false;
                }
            }, name + "-IOInbound");

            // For reading messages assume maximum priority: These are placed
            // into
            // the input queue, where message priority will dictate broker
            // dispatch
            // priority. Note that flow control from the input queue will limit
            // dispatch
            // of lower priority messages:
            if (MockBrokerTest.USE_INPUT_QUEUES) {
                dispatchContext.updatePriority(Message.MAX_PRIORITY);
            }
            dispatchContext.requestDispatch();

        }

        public FlowController<Message> getFlowController(Flow flow) {
            if (this.flow != null) {
                return flowController;
            } else {
                return super.getFlowController(flow);
            }
        }

        public void blockingDispatch() throws InterruptedException {
            Message m = getNextMessage();
            dispatch(m, getFlowController(m.getFlow()));
        }

        public void dispatch(Message m, FlowController<Message> controller) {

            switch (m.type()) {
            case Message.TYPE_FLOW_CONTROL: {
                FlowMessage fm = (FlowMessage) m;
                synchronized (output) {
                    try {
                        FlowController<Message> outputController = output.getFlowController(fm.getFlow());
                        ProtocolLimiter pl = (ProtocolLimiter) outputController.getLimiter();
                        synchronized (output) {
                            pl.onProtocolMessage(fm);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
            case Message.TYPE_FLOW_OPEN: {
                FlowController<Message> inputController = new FlowController<Message>(this, m.getFlow(), new WindowLimiter<Message>(false, m.getFlow(), inputWindowSize, inputResumeThreshold), this);
                // Allow overflow we should be limited by protocol:
                inputController.useOverFlowQueue(false);
                super.onFlowOpened(inputController);
                break;
            }
            case Message.TYPE_FLOW_CLOSE: {
                super.onFlowClosed(m.getFlow());
                break;
            }
            default: {
                if (inputQueue != null) {
                    inputQueue.add(m, null);
                } else {
                    controller.add(m, null);
                }
            }
            }
        }

        public void addFlowReadyListener(final IPollableFlowSource.FlowReadyListener<Message> listener) {
            addReadReadyListener(new ReadReadyListener() {

                public void onReadReady() {
                    listener.onFlowReady(NetworkSource.this);
                }

            });
        }

        public boolean pollingDispatch() {

            Message m = pollNextMessage();
            if (m != null) {
                dispatch(m, getFlowController(m.getFlow()));
                return true;
            }
            return false;
        }

        // Called by FlowController.add()....
        public void flowElemAccepted(final ISourceController<Message> controller, final Message elem) {
            messageReceived(elem, controller);
            controller.elementDispatched(elem);
        }

        public IFlowSink<Message> getFlowSink() {
            // No sink, this is a source only:
            return null;
        }

        public IFlowSource<Message> getFlowSource() {
            return this;
        }

        public boolean isDispatchReady() {
            return true;
        }
    }

    public class FlowMessage extends Message {
        int size;
        int count;

        FlowMessage(Flow flow, int size, int count) {
            super(0, 0, null, flow, null, 0);
            this.size = size;
            this.count = count;
        }

        public short type() {
            return TYPE_FLOW_CONTROL;
        }

        public boolean isSystem() {
            return true;
        }

        public int getSize() {
            return size;
        }
    }

    public class FlowOpen extends Message {
        FlowOpen(Flow flow) {
            super(0, 0, null, flow, null, 0);
        }

        public short type() {
            return TYPE_FLOW_OPEN;
        }

        public boolean isSystem() {
            return true;
        }
    }

    public class FlowClose extends Message {
        FlowClose(Flow flow) {
            super(0, 0, null, flow, null, 0);
        }

        public short type() {
            return TYPE_FLOW_CLOSE;
        }

        public boolean isSystem() {
            return true;
        }
    }

    public interface ReadReadyListener {
        public void onReadReady();
    }

    public String getName() {
        return name;
    }

}
