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

import org.apache.activemq.dispatch.DispatchOption;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSource;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatcherConfig;
import org.apache.activemq.dispatch.internal.BaseSuspendable;
import org.apache.activemq.dispatch.internal.nio.NioDispatchSource;

import static org.apache.activemq.dispatch.DispatchPriority.*;

final public class AdvancedDispatcher extends BaseSuspendable implements Dispatcher {

    public final static ThreadLocal<DispatchQueue> CURRENT_QUEUE = new ThreadLocal<DispatchQueue>();

    final SerialDispatchQueue mainQueue;
    final GlobalDispatchQueue globalQueues[];
    final AtomicLong globalQueuedRunnables = new AtomicLong();

    private final ArrayList<DispatcherThread> dispatchers = new ArrayList<DispatcherThread>();

    private int roundRobinCounter = 0;
    private int size;
    private final int numPriorities;

    protected LoadBalancer loadBalancer;

    public AdvancedDispatcher(DispatcherConfig config) {
        this.size = config.getThreads();
        this.numPriorities = 3;
        this.mainQueue = new SerialDispatchQueue(this, "main");
        globalQueues = new GlobalDispatchQueue[3];
        for (int i = 0; i < 3; i++) {
            globalQueues[i] = new GlobalDispatchQueue(this, DispatchPriority.values()[i]);
        }
        loadBalancer = new SimpleLoadBalancer();
        super.suspend();
    }
    
    @Override
    protected void onStartup() {
        for (int i = 0; i < size; i++) {
            DispatcherThread dispatacher;
            try {
                dispatacher = new DispatcherThread(this, ("dispatcher -" + (i + 1)), numPriorities);
                dispatchers.add(dispatacher);
                dispatacher.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void retain() {
        super.retain();
    }
    
    @Override
    public void suspend() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void onShutdown() {
        Runnable countDown = new Runnable() {
            AtomicInteger shutdownCountDown = new AtomicInteger(dispatchers.size());

            public void run() {
                if (shutdownCountDown.decrementAndGet() == 0) {
                    // Notify any registered shutdown watchers.
                    AdvancedDispatcher.super.onShutdown();
                }
            }
        };

        for (DispatcherThread d : new ArrayList<DispatcherThread>(dispatchers)) {
            d.shutdown(countDown);
        }
        loadBalancer.stop();
    }
    

    /**
     * A Dispatcher must call this to indicate that is has started it's dispatch
     * loop.
     */
    public void onDispatcherStarted(DispatcherThread d) {
        DispatcherThread.CURRENT.set(d);
        loadBalancer.onDispatcherStarted(d);
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * A Dispatcher must call this when exiting it's dispatch loop
     */
    public void onDispatcherStopped(DispatcherThread d) {
        synchronized (dispatchers) {
            if (dispatchers.remove(d)) {
                size--;
            }
        }
        loadBalancer.onDispatcherStopped(d);
    }

    protected DispatcherThread chooseDispatcher() {
        DispatcherThread d = DispatcherThread.CURRENT.get();
        if (d == null) {
            synchronized (dispatchers) {
                if (dispatchers.isEmpty()) {
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

    // public DispatchContext register(Runnable runnable, String name) {
    // return chooseDispatcher().register(runnable, name);
    // }

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

    public DispatchQueue getGlobalQueue() {
        return getGlobalQueue(DEFAULT);
    }

    public DispatchQueue getGlobalQueue(DispatchPriority priority) {
        return globalQueues[priority.ordinal()];
    }

    public DispatchQueue createSerialQueue(String label, DispatchOption... options) {
        SerialDispatchQueue rc = new SerialDispatchQueue(this, label, options);
        rc.setTargetQueue(getGlobalQueue());
        return rc;
    }

    public void dispatchMain() {
        mainQueue.run();
    }

    public DispatchSource createSource(SelectableChannel channel, int interestOps, DispatchQueue queue) {
        NioDispatchSource source = new NioDispatchSource(this, channel, interestOps);
        source.setTargetQueue(queue);
        return source;
    }

    public DispatchQueue getCurrentQueue() {
        return CURRENT_QUEUE.get();
    }

    public DispatchQueue getCurrentThreadQueue() {
        DispatcherThread thread = DispatcherThread.CURRENT.get();
        if (thread == null) {
            return null;
        }
        return thread.currentDispatchQueue;
    }

}
