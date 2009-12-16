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

package org.apache.activemq.dispatch.internal.simple;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class DispatcherThread extends Thread {

    private static final int MAX_LOCAL_DISPATCH_BEFORE_CHECKING_GLOBAL = 1000;
    private final SimpleDispatcher dispatcher;
    final ThreadDispatchQueue[] threadQueues;
    final AtomicLong threadQueuedRunnables = new AtomicLong();
    final IntegerCounter executionCounter = new IntegerCounter();
    ThreadDispatchQueue currentThreadQueue;

    public DispatcherThread(SimpleDispatcher dispatcher, int ordinal) {
        this.dispatcher = dispatcher;
        this.threadQueues = new ThreadDispatchQueue[dispatcher.globalQueues.length];
        for (int i = 0; i < threadQueues.length; i++) {
            threadQueues[i] = new ThreadDispatchQueue(this, dispatcher.globalQueues[i]);
        }
        setName(dispatcher.getLabel() + " dispatcher: " + (ordinal + 1));
        setDaemon(true);
    }

    @Override
    public void run() {
        GlobalDispatchQueue[] globalQueues = dispatcher.globalQueues;
        final int PRIORITIES = threadQueues.length;
        int processGlobalQueueCount = PRIORITIES;

        try {
            start: for (;;) {

                executionCounter.set(MAX_LOCAL_DISPATCH_BEFORE_CHECKING_GLOBAL);

                // Process the local non-synchronized queues.
                // least contention
                outer: while (executionCounter.get() > 0) {
                    processGlobalQueueCount = PRIORITIES;
                    for (int i = 0; i < PRIORITIES; i++) {
                        currentThreadQueue = threadQueues[i];
                        Runnable runnable = currentThreadQueue.pollLocal();
                        if (runnable == null) {
                            continue;
                        }

                        SimpleDispatcher.CURRENT_QUEUE.set(currentThreadQueue.globalQueue);
                        processGlobalQueueCount = i;
                        for (;;) {
                            dispatch(runnable);
                            if (executionCounter.decrementAndGet() <= 0) {
                                break outer;
                            }
                            runnable = currentThreadQueue.pollLocal();
                            if (runnable == null) {
                                break;
                            }
                        }
                    }

                    // There was no work to do in the local queues..
                    if (processGlobalQueueCount == PRIORITIES) {
                        break;
                    }
                }

                // Process the local synchronized queues.
                // medium contention
                outer: while (executionCounter.get() > 0) {
                    processGlobalQueueCount = PRIORITIES;
                    for (int i = 0; i < PRIORITIES; i++) {
                        currentThreadQueue = threadQueues[i];
                        Runnable runnable = currentThreadQueue.pollShared();
                        if (runnable == null) {
                            continue;
                        }
                        SimpleDispatcher.CURRENT_QUEUE.set(currentThreadQueue.globalQueue);
                        processGlobalQueueCount = i;
                        for (;;) {
                            dispatch(runnable);
                            if (executionCounter.decrementAndGet() <= 0) {
                                break outer;
                            }
                            runnable = currentThreadQueue.pollShared();
                            if (runnable == null) {
                                break;
                            }
                        }
                    }

                    // There was no work to do in the local queues..
                    if (processGlobalQueueCount == PRIORITIES) {
                        break;
                    }
                }

                // Process the global synchronized queues.
                // most contention
                for (int i = 0; i < processGlobalQueueCount; i++) {
                    currentThreadQueue = threadQueues[i];
                    GlobalDispatchQueue queue = globalQueues[i];
                    Runnable runnable = queue.poll();
                    if (runnable == null) {
                        continue;
                    }
                    // We only execute 1 global runnable at a time,
                    // hoping it generates local work for us.
                    SimpleDispatcher.CURRENT_QUEUE.set(queue);
                    dispatch(runnable);
                    continue start;
                }

                if (executionCounter.get() != MAX_LOCAL_DISPATCH_BEFORE_CHECKING_GLOBAL) {
                    continue start;
                }

                // If we get here then there was no work in the global queues..
                try {
                    waitForWakeup();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        } catch (Shutdown e) {
        }
    }

    @SuppressWarnings("serial")
    static class Shutdown extends RuntimeException {
    }

    private void dispatch(Runnable runnable) {
        try {
            runnable.run();
        } catch (Shutdown e) {
            throw e;
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static DispatcherThread currentDispatcherThread() {
        Thread currentThread = Thread.currentThread();
        if (currentThread.getClass() == DispatcherThread.class) {
            return (DispatcherThread) currentThread;
        }
        return null;
    }

    private final Semaphore wakeups = new Semaphore(0);
    private final AtomicBoolean inWaitingList = new AtomicBoolean(false);

    private void waitForWakeup() throws InterruptedException {
        while (threadQueuedRunnables.get() == 0 && dispatcher.globalQueuedRunnables.get() == 0) {
            if (!wakeups.tryAcquire()) {
                if (inWaitingList.compareAndSet(false, true)) {
                    if (!dispatcher.addWaitingDispatcher(this)) {
                        inWaitingList.set(false);
                    }
                }
            }
            wakeups.acquire();
        }
        wakeups.drainPermits();
    }

    public void globalWakeup() {
        wakeups.release();
        inWaitingList.set(false);
    }

    public void wakeup() {
        wakeups.release();
    }

}