package org.apache.activemq.dispatch;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractPooledDispatcher<D extends IDispatcher> implements IDispatcher, PooledDispatcher<D> {
    
    private final String name;

    private final ThreadLocal<D> dispatcher = new ThreadLocal<D>();
    private final ThreadLocal<PooledDispatchContext<D>> dispatcherContext = new ThreadLocal<PooledDispatchContext<D>>();
    private final ArrayList<D> dispatchers = new ArrayList<D>();

    final AtomicBoolean started = new AtomicBoolean();
    final AtomicBoolean shutdown = new AtomicBoolean();

    private int roundRobinCounter = 0;
    private final int size;

    protected ExecutionLoadBalancer<D> loadBalancer;

    protected AbstractPooledDispatcher(String name, int size) {
        this.name = name;
        this.size = size;
        loadBalancer = new SimpleLoadBalancer<D>();
    }

    /**
     * Subclasses should implement this to return a new dispatcher.
     * 
     * @param name
     *            The name to assign the dispatcher.
     * @param pool
     *            The pool.
     * @return The new dispathcer.
     */
    protected abstract D createDispatcher(String name, AbstractPooledDispatcher<D> pool) throws Exception;

    /**
     * @see org.apache.activemq.dispatch.IDispatcher#start()
     */
    public synchronized final void start() throws Exception {
        loadBalancer.start();
        if (started.compareAndSet(false, true)) {
            // Create all the workers.
            try {
                for (int i = 0; i < size; i++) {
                    D dispatacher = createDispatcher(name + "-" + (i + 1), this);

                    dispatchers.add(dispatacher);
                    dispatacher.start();
                }
            } catch (Exception e) {
                shutdown();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#shutdown()
     */
    public synchronized final void shutdown() throws InterruptedException {
        shutdown.set(true);
        boolean interrupted = false;
        while (!dispatchers.isEmpty()) {
            try {
                dispatchers.get(dispatchers.size() - 1).shutdown();
            } catch (InterruptedException ie) {
                interrupted = true;
                continue;
            }
            dispatchers.remove(dispatchers.size() - 1);

        }
        // Re-interrupt:
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        loadBalancer.stop();
    }

    public void setCurrentDispatchContext(PooledDispatchContext<D> context) {
        dispatcherContext.set(context);
    }

    public PooledDispatchContext<D> getCurrentDispatchContext() {
        return dispatcherContext.get();
    }

    /**
     * Returns the currently executing dispatcher, or null if the current thread
     * is not a dispatcher:
     * 
     * @return The currently executing dispatcher
     */
    public D getCurrentDispatcher() {
        return dispatcher.get();
    }

    /**
     * A Dispatcher must call this to indicate that is has started it's dispatch
     * loop.
     */
    public void onDispatcherStarted(D d) {
        dispatcher.set(d);
        loadBalancer.addDispatcher(d);
    }

    public ExecutionLoadBalancer<D> getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * A Dispatcher must call this when exiting it's dispatch loop
     */
    public void onDispatcherStopped(D d) {
        loadBalancer.removeDispatcher(d);
    }

    protected D chooseDispatcher() {
        D d = dispatcher.get();
        if (d == null) {
            synchronized (dispatchers) {
                if (++roundRobinCounter >= size) {
                    roundRobinCounter = 0;
                }
                return dispatchers.get(roundRobinCounter);
            }
        } else {
            return d;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.dispatch.IDispatcher#schedule(java.lang.Runnable,
     * long, java.util.concurrent.TimeUnit)
     */
    public void schedule(final Runnable runnable, long delay, TimeUnit timeUnit) {
        chooseDispatcher().schedule(runnable, delay, timeUnit);
    }

    public DispatchContext register(Dispatchable dispatchable, String name) {
        return chooseDispatcher().register(dispatchable, name);
    }

    public String toString() {
        return name;
    }

}
