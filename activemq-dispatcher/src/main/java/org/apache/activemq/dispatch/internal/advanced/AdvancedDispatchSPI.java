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

import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSPI;
import org.apache.activemq.dispatch.DispatchSource;
import org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority;
import org.apache.activemq.dispatch.internal.SerialDispatchQueue;

import static org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority.*;

public class AdvancedDispatchSPI implements DispatchSPI {

    final SerialDispatchQueue mainQueue = new SerialDispatchQueue("main");
    final GlobalDispatchQueue globalQueues[];
    final AtomicLong globalQueuedRunnables = new AtomicLong();

    private final ThreadLocal<DispatcherThread> dispatcher = new ThreadLocal<DispatcherThread>();
    private final ThreadLocal<DispatchContext> dispatcherContext = new ThreadLocal<DispatchContext>();
    private final ArrayList<DispatcherThread> dispatchers = new ArrayList<DispatcherThread>();

    final AtomicInteger startCounter = new AtomicInteger();
//    final AtomicBoolean started = new AtomicBoolean();
//    final AtomicBoolean shutdown = new AtomicBoolean();

    private int roundRobinCounter = 0;
    private int size;
    private final int numPriorities;

    protected LoadBalancer loadBalancer;

    public AdvancedDispatchSPI(int size, int numPriorities) {
        this.size = size;
        this.numPriorities = numPriorities;
        
        globalQueues = new GlobalDispatchQueue[3];
        for (int i = 0; i < 3; i++) {
            globalQueues[i] = new GlobalDispatchQueue(this, DispatchQueuePriority.values()[i]);
        }
        
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
    protected DispatcherThread createDispatcher(String name) throws Exception {
        return new DispatcherThread(this, name, numPriorities);
    }

    /**
     * @see org.apache.activemq.dispatch.internal.advanced.Dispatcher#start()
     */
    public synchronized final void start()  {
        if( startCounter.getAndIncrement()==0 ) {
            // Create all the workers.
            try {
                loadBalancer.start();
                for (int i = 0; i < size; i++) {
                    DispatcherThread dispatacher = createDispatcher("dispatcher -" + (i + 1));
                    dispatchers.add(dispatacher);
                    dispatacher.start();
                }
            } catch (Exception e) {
                shutdown();
            }
        }
    }

    public final void shutdown() {
        shutdown(null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#shutdown()
     */
    public final void shutdown(Runnable onShutdown) {
        if( startCounter.decrementAndGet()==0 ) {
            final AtomicInteger shutdownCountDown = new AtomicInteger(dispatchers.size());
            for (DispatcherThread d : new ArrayList<DispatcherThread>(dispatchers)) {
                d.shutdown(shutdownCountDown, onShutdown);
            }
            loadBalancer.stop();
        }
    }

    public void setCurrentDispatchContext(DispatchContext context) {
        dispatcherContext.set(context);
    }

    public DispatchContext getCurrentDispatchContext() {
        return dispatcherContext.get();
    }

    /**
     * Returns the currently executing dispatcher, or null if the current thread
     * is not a dispatcher:
     * 
     * @return The currently executing dispatcher
     */
    public Dispatcher getCurrentDispatcher() {
        return dispatcher.get();
    }

    /**
     * A Dispatcher must call this to indicate that is has started it's dispatch
     * loop.
     */
    public void onDispatcherStarted(DispatcherThread d) {
        dispatcher.set(d);
        loadBalancer.onDispatcherStarted(d);
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * A Dispatcher must call this when exiting it's dispatch loop
     */
    public void onDispatcherStopped(Dispatcher d) {
        synchronized (dispatchers) {
            if (dispatchers.remove(d)) {
                size--;
            }
        }
        loadBalancer.onDispatcherStopped(d);
    }

    protected DispatcherThread chooseDispatcher() {
        DispatcherThread d = dispatcher.get();
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

    public DispatchContext register(Runnable runnable, String name) {
        return chooseDispatcher().register(runnable, name);
    }

	public int getSize() {
		return size;
	}
	
    public final Executor createPriorityExecutor(final int priority) {
        return new Executor() {
            public void execute(final Runnable runnable) {
                chooseDispatcher().dispatch(runnable, priority);
            }

        };
    }

    public int getDispatchPriorities() {
        // TODO Auto-generated method stub
        return numPriorities;
    }

    public void execute(Runnable command) {
        chooseDispatcher().dispatch(command, 0);
    }
    
    public void execute(Runnable command, int priority) {
        chooseDispatcher().dispatch(command, priority);
    }

    public void schedule(final Runnable runnable, long delay, TimeUnit timeUnit) {
        chooseDispatcher().schedule(runnable, delay, timeUnit);
    }

    public void schedule(final Runnable runnable, int priority, long delay, TimeUnit timeUnit) {
        chooseDispatcher().schedule(runnable, priority, delay, timeUnit);
    }
    
    public DispatchQueue getMainQueue() {
        return mainQueue;
    }
    
    public DispatchQueue getGlobalQueue(DispatchQueuePriority priority) {
        return globalQueues[priority.ordinal()];
    }
    
    public DispatchQueue createQueue(String label) {
        SerialDispatchQueue rc = new SerialDispatchQueue(label);
        rc.setTargetQueue(getGlobalQueue(DEFAULT));
        return rc;
    }
    
    public void dispatchMain() {
        mainQueue.run();
    }

    public DispatchSource createSource(SelectableChannel channel, int interestOps, DispatchQueue queue) {
        return null;
    }    

}
