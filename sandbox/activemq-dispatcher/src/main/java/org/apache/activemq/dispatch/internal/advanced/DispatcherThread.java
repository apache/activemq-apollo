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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSource;
import org.apache.activemq.dispatch.internal.nio.NioSelector;
import org.apache.activemq.util.Mapper;
import org.apache.activemq.util.PriorityLinkedList;
import org.apache.activemq.util.TimerHeap;
import org.apache.activemq.util.list.LinkedNodeList;

public class DispatcherThread implements Runnable {

    static public final ThreadLocal<DispatcherThread> CURRENT = new ThreadLocal<DispatcherThread>();

    final ThreadDispatchQueue dispatchQueues[];
    
    static final boolean DEBUG = false;
    private Thread thread;
    protected boolean running = false;
    private boolean threaded = false;
    protected final int MAX_USER_PRIORITY;
    protected final HashSet<DispatchContext> contexts = new HashSet<DispatchContext>();

    // Set if this dispatcher is part of a dispatch pool:
    protected final AdvancedDispatcher dispatcher;

    // The local dispatch queue:
    protected final PriorityLinkedList<DispatchContext> priorityQueue;

    // Dispatch queue for requests from other threads:
    final LinkedNodeList<ForeignEvent>[] foreignQueue = createForeignQueue();
    
    ThreadDispatchQueue currentDispatchQueue;

    private static final int[] TOGGLE = new int[] { 1, 0 };
    int foreignToggle = 0;

    // Timed Execution List
    protected final TimerHeap<Runnable> timerHeap = new TimerHeap<Runnable>() {
        @Override
        protected final void execute(Runnable ready) {
            ready.run();
        }
    };

    protected final String name;
    final AtomicBoolean foreignAvailable = new AtomicBoolean(false);
    private final Semaphore foreignPermits = new Semaphore(0);

    private final Mapper<Integer, DispatchContext> PRIORITY_MAPPER = new Mapper<Integer, DispatchContext>() {
        public Integer map(DispatchContext element) {
            return element.listPrio;
        }
    };
    

    private final NioSelector nioHandler;

    protected DispatcherThread(AdvancedDispatcher dispatcher, String name, int priorities) throws IOException {
        this.name = name;
        this.nioHandler = new NioSelector();
        this.dispatchQueues = new ThreadDispatchQueue[3];
        for (int i = 0; i < 3; i++) {
            dispatchQueues[i] = new ThreadDispatchQueue(this, DispatchPriority.values()[i]);
        }

        MAX_USER_PRIORITY = priorities - 1;
        priorityQueue = new PriorityLinkedList<DispatchContext>(MAX_USER_PRIORITY + 1, PRIORITY_MAPPER);
        for (int i = 0; i < 2; i++) {
            foreignQueue[i] = new LinkedNodeList<ForeignEvent>();
        }
        this.dispatcher = dispatcher;
    }
    
    @SuppressWarnings("unchecked")
    private LinkedNodeList<ForeignEvent>[] createForeignQueue() {
        return new LinkedNodeList[2];
    }

    public boolean isThreaded() {
        return threaded;
    }

    public void setThreaded(boolean threaded) {
        this.threaded = threaded;
    }

    public int getDispatchPriorities() {
        return MAX_USER_PRIORITY;
    }

    public DispatchContext register(Runnable runnable, String name) {
        return new DispatchContext(this, runnable, true, name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#start()
     */
    public synchronized final void start() {
        if (thread == null) {
            running = true;
            thread = new Thread(this, name);
            thread.start();
        }
    }
    
    public Thread shutdown(final Runnable onShutdown) {
        synchronized (this) {
            if (thread != null) {
                dispatchInternal(new Runnable() {
                    public void run() {
                        running = false;
                        if( onShutdown!=null ) {
                            onShutdown.run();
                        }
                        try {
                            nioHandler.shutdown();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }, MAX_USER_PRIORITY + 1);
                Thread rc = thread;
                thread = null;
                return rc;
            } else {
                if( onShutdown!=null) {
                    onShutdown.run();
                }
            }
            return null;
        }
    }

    protected void cleanup() {
        ArrayList<DispatchContext> toClose = null;
        synchronized (this) {
            running = false;
            toClose = new ArrayList<DispatchContext>(contexts.size());
            toClose.addAll(contexts);
        }

        for (DispatchContext context : toClose) {
            context.close(false);
        }
    }

    public void run() {

        dispatcher.onDispatcherStarted((DispatcherThread) this);
        DispatchContext pdc;
        try {
            while (running) {
                int counter = 0;
                // If no local work available wait for foreign work:
                while((pdc = priorityQueue.poll())!=null){
                    if( pdc.priority < dispatchQueues.length ) {
                        currentDispatchQueue = dispatchQueues[pdc.priority];
                        AdvancedDispatcher.CURRENT_QUEUE.set(currentDispatchQueue);
                    }
                    
                    if (pdc.tracker != null) {
                        DispatchContext.CURRENT.set(pdc);
                    }

                    counter++;
                    pdc.run();

                    if (pdc.tracker != null) {
                        DispatchContext.CURRENT.set(null);
                    }
                }

                if( counter==0 ) {
                    waitForEvents();
                }

                // Execute delayed events:
                timerHeap.executeReadyTimers();

                // Allow subclasses to do additional work:
                dispatchHook();

                // Check for foreign dispatch requests:
                if (foreignAvailable.get()) {
                    LinkedNodeList<ForeignEvent> foreign;
                    synchronized (foreignQueue) {
                        // Swap foreign queues and drain permits;
                        foreign = foreignQueue[foreignToggle];
                        foreignToggle = TOGGLE[foreignToggle];
                        foreignAvailable.set(false);
                        foreignPermits.drainPermits();
                    }
                    while (true) {
                        ForeignEvent fe = foreign.getHead();
                        if (fe == null) {
                            break;
                        }

                        fe.unlink();
                        fe.run();
                    }
                }
            }
        } catch (InterruptedException e) {
            return;
        } catch (Throwable thrown) {
            thrown.printStackTrace();
        } finally {
            dispatcher.onDispatcherStopped((DispatcherThread) this);
            cleanup();
        }
    }

    /**
     * Subclasses may override this to do do additional dispatch work:
     */
    protected void dispatchHook() throws Exception {

    }

    /**
     * Subclasses may override this to implement another mechanism for wakeup.
     * 
     * @throws Exception
     */
    protected void waitForEvents() throws Exception {
        long next = timerHeap.timeToNext(TimeUnit.NANOSECONDS);
        if (next == -1) {
            foreignPermits.acquire();
        } else if (next > 0) {
            foreignPermits.tryAcquire(next, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Subclasses may override this to provide an alternative wakeup mechanism.
     */
    protected void wakeup() {
        foreignPermits.release();
    }

    protected void onForeignUpdate(DispatchContext context) {
        synchronized (foreignQueue) {

            ForeignEvent fe = context.updateEvent[foreignToggle];
            if (!fe.isLinked()) {
                foreignQueue[foreignToggle].addLast(fe);
                if (!foreignAvailable.getAndSet(true)) {
                    wakeup();
                }
            }
        }
    }

    protected boolean removeDispatchContext(DispatchContext context) {
        synchronized (foreignQueue) {

            if (context.updateEvent[0].isLinked()) {
                context.updateEvent[0].unlink();
            }
            if (context.updateEvent[1].isLinked()) {
                context.updateEvent[1].unlink();
            }
        }

        if (context.isLinked()) {
            context.unlink();
            return true;
        }

        synchronized (this) {
            contexts.remove(context);
        }

        return false;
    }

    protected boolean takeOwnership(DispatchContext context) {
        synchronized (this) {
            if (running) {
                contexts.add(context);
            } else {
                return false;
            }
        }
        return true;
    }

    //Special dispatch method that allow high priority dispatch:
    private void dispatchInternal(Runnable runnable, int priority) {
        DispatchContext context = new DispatchContext(this, runnable, false, name);
        context.priority = priority;
        context.requestDispatch();
    }

    public void dispatch(Runnable runnable, int priority) {
        DispatchContext context = new DispatchContext(this, runnable, false, name);
        context.updatePriority(priority);
        context.requestDispatch();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.IDispatcher#createPriorityExecutor(int)
     */
    public Executor createPriorityExecutor(final int priority) {

        return new Executor() {

            public void execute(final Runnable runnable) {
                dispatch(runnable, priority);
            }
        };
    }

    public void execute(Runnable runnable) {
        dispatch(runnable, 0);
    }
    
    public void execute(Runnable runnable, int prio) {
        dispatch(runnable, prio);
    }

    public void schedule(final Runnable runnable, final long delay, final TimeUnit timeUnit) {
        schedule(runnable, 0, delay, timeUnit);
    }
    
    public void schedule(final Runnable runnable, final int prio, final long delay, final TimeUnit timeUnit) {
        final Runnable wrapper = new Runnable() {
            public void run() {
                execute(runnable, prio);
            }
        };
        if (DispatcherThread.CURRENT.get() == this) {
            timerHeap.addRelative(wrapper, delay, timeUnit);
        } else {
            add(new TimerEvent(this, wrapper, delay, timeUnit));
        }
    }
    
    void add(ForeignEvent event) {
        synchronized (foreignQueue) {
            if (!event.isLinked()) {
                foreignQueue[foreignToggle].addLast(event);
                if (!foreignAvailable.getAndSet(true)) {
                    wakeup();
                }
            }
        }
    }


    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }
}
