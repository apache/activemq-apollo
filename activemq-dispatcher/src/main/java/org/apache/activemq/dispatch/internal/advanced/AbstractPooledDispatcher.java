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
package org.apache.activemq.dispatch.internal.advanced;

import java.util.ArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractPooledDispatcher implements IDispatcher, PooledDispatcher {

    private final String name;

    private final ThreadLocal<IDispatcher> dispatcher = new ThreadLocal<IDispatcher>();
    private final ThreadLocal<PooledDispatchContext> dispatcherContext = new ThreadLocal<PooledDispatchContext>();
    private final ArrayList<IDispatcher> dispatchers = new ArrayList<IDispatcher>();

    final AtomicBoolean started = new AtomicBoolean();
    final AtomicBoolean shutdown = new AtomicBoolean();

    private int roundRobinCounter = 0;
    private int size;

    protected ExecutionLoadBalancer loadBalancer;

    protected AbstractPooledDispatcher(String name, int size) {
        this.name = name;
        this.size = size;
        loadBalancer = new SimpleLoadBalancer();
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
    protected abstract IDispatcher createDispatcher(String name, AbstractPooledDispatcher pool) throws Exception;

    /**
     * @see org.apache.activemq.dispatch.internal.advanced.IDispatcher#start()
     */
    public synchronized final void start() throws Exception {
        loadBalancer.start();
        if (started.compareAndSet(false, true)) {
            // Create all the workers.
            try {
                for (int i = 0; i < size; i++) {
                    IDispatcher dispatacher = createDispatcher(name + "-" + (i + 1), this);

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
        }
        // Re-interrupt:
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        loadBalancer.stop();
    }

    public void setCurrentDispatchContext(PooledDispatchContext context) {
        dispatcherContext.set(context);
    }

    public PooledDispatchContext getCurrentDispatchContext() {
        return dispatcherContext.get();
    }

    /**
     * Returns the currently executing dispatcher, or null if the current thread
     * is not a dispatcher:
     * 
     * @return The currently executing dispatcher
     */
    public IDispatcher getCurrentDispatcher() {
        return dispatcher.get();
    }

    /**
     * A Dispatcher must call this to indicate that is has started it's dispatch
     * loop.
     */
    public void onDispatcherStarted(IDispatcher d) {
        dispatcher.set(d);
        loadBalancer.onDispatcherStarted(d);
    }

    public ExecutionLoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * A Dispatcher must call this when exiting it's dispatch loop
     */
    public void onDispatcherStopped(IDispatcher d) {
        synchronized (dispatchers) {
            if (dispatchers.remove(d)) {
                size--;
            }
        }
        loadBalancer.onDispatcherStopped(d);
    }

    protected IDispatcher chooseDispatcher() {
        IDispatcher d = dispatcher.get();
        if (d == null) {
            synchronized (dispatchers) {
                if(dispatchers.isEmpty())
                {
                    throw new RejectedExecutionException();
                }
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

	public String getName() {
		return name;
	}

	public int getSize() {
		return size;
	}

}
