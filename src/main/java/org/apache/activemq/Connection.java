package org.apache.activemq;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

abstract public class Connection implements TransportListener {

    protected Transport transport;
    protected String name;

    private int priorityLevels;
    protected int outputWindowSize = 1000;
    protected int outputResumeThreshold = 900;
    protected int inputWindowSize = 1000;
    protected int inputResumeThreshold = 500;
    
    private IDispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private  ExecutorService blockingWriter;

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void start() throws Exception {
        transport.setTransportListener(this);
        if (transport instanceof DispatchableTransport) {
            DispatchableTransport dt = ((DispatchableTransport) transport);
            if (name != null) {
                dt.setName(name);
            }
            dt.setDispatcher(getDispatcher());
        } else {
            blockingWriter = Executors.newSingleThreadExecutor();
        }
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

    protected void initialize() {
    }
    
    protected final void write(final Object o) {
        if (blockingWriter==null) {
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
                //Must be shutting down.
            }
        }
    }
    
    public void onException(IOException error) {
        onException((Exception) error);
    }

    public void onException(Exception error) {
        if (!isStopping()) {
            System.out.println("RemoteConnection error: " + error);
            error.printStackTrace();
        }
    }

    public boolean isStopping(){ 
        return stopping.get();
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

    protected interface ProtocolLimiter<E> extends IFlowLimiter<E> {
        public void onProtocolCredit(int credit);
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

        public void reserve(E elem) {
            super.reserve(elem);
//            if (!clientMode) {
//                 System.out.println(name + " Reserved " + this);
//            }
        }

        public void releaseReserved(E elem) {
            super.reserve(elem);
//            if (!clientMode) {
//                System.out.println(name + " Released Reserved " + this);
//            }
        }

        protected void remove(int size) {
            super.remove(size);
            if (!clientMode) {
                available += size;
                if (available >= capacity - resumeThreshold) {
                    sendCredit(available);
                    available = 0;
                }
            }
        }

        protected void sendCredit(int credit) {
            throw new UnsupportedOperationException("Please override this method to provide and implemenation.");
        }

        public void onProtocolCredit(int credit) {
            remove(credit);
        }

        public int getElementSize(MessageDelivery m) {
            return m.getFlowLimiterSize();
        }
    }

}
