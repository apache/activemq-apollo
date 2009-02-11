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
package org.apache.activemq.dispatch;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PriorityPooledDispatcher implements IDispatcher {
    private final String name;

    final AtomicBoolean started = new AtomicBoolean();
    final AtomicBoolean shutdown = new AtomicBoolean();

    ArrayList<PriorityDispatcher> dispatchers = new ArrayList<PriorityDispatcher>();
    private int roundRobinCounter = 0;
    private final int size;

    private final SimpleLoadBalancer executionGraphLoadBalancer;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#createPriorityExecutor(int)
     */
    public Executor createPriorityExecutor(final int priority) {
        return new Executor() {
            public void execute(final Runnable runnable) {
                chooseDispatcher().dispatch(new RunnableAdapter(runnable), 0);
            }
        };
    }

    public PriorityPooledDispatcher(String name, int size, int priorities) {
        this.name = name;
        this.size = size;
        executionGraphLoadBalancer = new SimpleLoadBalancer(name);
        // Create all the workers.
        for (int i = 0; i < size; i++) {
            PriorityDispatcher dispatcher = new PriorityDispatcher(name + "-" + (i + 1), priorities, executionGraphLoadBalancer);
            dispatchers.add(dispatcher);
        }
    }

    public DispatchContext register(Dispatchable dispatchable, String name) {
        return chooseDispatcher().register(dispatchable, name);
    }

    /**
     * @see org.apache.activemq.dispatch.IDispatcher#start()
     */
    public synchronized final void start() {
        if (started.compareAndSet(false, true)) {
            // Create all the workers.
            for (int i = 0; i < size; i++) {
                dispatchers.get(i).start();
            }
        }
        try {
            executionGraphLoadBalancer.start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#shutdown()
     */
    public synchronized final void shutdown() throws InterruptedException {
        shutdown.set(true);
        for (PriorityDispatcher dispatcher : dispatchers) {
            dispatcher.shutdown();
        }
        executionGraphLoadBalancer.shutdown();
    }

    private PriorityDispatcher chooseDispatcher() {
        PriorityDispatcher d = PriorityDispatcher.dispatcher.get();
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

    public void execute(final Runnable runnable) {
        chooseDispatcher().dispatch(new RunnableAdapter(runnable), 0);
    }

    // TODO Implement
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

    public String toString() {
        return name;
    }
}
