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
package org.apache.activemq.dispatch.internal.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.dispatch.internal.advanced.AdvancedDispatcher;
import org.apache.activemq.dispatch.internal.advanced.DispatcherThread;

public class NIODispatcherThread extends DispatcherThread {
    private final boolean DEBUG = false;

    private final Selector selector;

    protected NIODispatcherThread(AdvancedDispatcher dispatcher, String name, int priorities) throws IOException {
        super(dispatcher, name, priorities);
        this.selector = Selector.open();
    }

    Selector getSelector() {
        return selector;
    }

    protected void cleanup() {
        try {
            selector.close();
        } catch (IOException e) {
            if (DEBUG) {
                debug("Error closing selector", e);
            }
        }
    }

    public SelectableDispatchContext registerSelectable(Runnable dispatchable, String name) {
        return new SelectableDispatchContext(this, dispatchable, name);
    }

    /**
     * Subclasses may override this to do do additional dispatch work:
     * 
     * @throws Exception
     */
    protected void dispatchHook() throws Exception {
        doSelect(true);
    }

    protected void waitForEvents() throws Exception {
        doSelect(false);
    }

    /**
     * Subclasses may override this to provide an alternative wakeup mechanism.
     */
    protected void wakeup() {
        selector.wakeup();
    }

    private long lastSelect = System.nanoTime();
    private long frequency = 50000000;

    private void doSelect(boolean now) throws IOException {

        // Select what's ready now:
        try {
            if (now) {
                if (selector.keys().isEmpty()) {
                    return;
                }
                // selector.selectNow();
                // processSelected();

                long time = System.nanoTime();
                if (time - lastSelect > frequency) {
                    selector.selectNow();
                    lastSelect = time;

                    int registered = selector.keys().size();
                    int selected = selector.selectedKeys().size();
                    if (selected == 0) {
                        frequency += 1000000;
                        if (DEBUG)
                            debug(this + "Increased select frequency to " + frequency);
                    } else if (selected > registered / 4) {
                        frequency -= 1000000;
                        if (DEBUG)
                            debug(this + "Decreased select frequency to " + frequency);
                    }
                    processSelected();

                }
            } else {
                long next = timerHeap.timeToNext(TimeUnit.MILLISECONDS);
                if (next == -1) {
                    selector.select();
                } else if (next > 0) {
                    selector.select(next);
                } else {
                    selector.selectNow();
                }
                lastSelect = System.nanoTime();
                processSelected();
            }

        } catch (CancelledKeyException ignore) {
            // A key may have been canceled.
        }

    }

    private void processSelected() {

        // Walk the set of ready keys servicing each ready context:
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        if (!selectedKeys.isEmpty()) {
            for (Iterator<SelectionKey> i = selectedKeys.iterator(); i.hasNext();) {
                boolean done = false;
                SelectionKey key = i.next();
                if (key.isValid()) {
                    SelectableDispatchContext context = (SelectableDispatchContext) key.attachment();

                    done = true;
                    try {
                        done = context.onSelect();
                    } catch (RuntimeException re) {
                        if (DEBUG)
                            debug("Exception in " + context + " closing");
                        // If there is a Runtime error close the context:
                        // TODO better error handling here:
                        context.close(false);
                    }
                } else {
                    done = true;
                }

                // If no more interests remove:
                if (done) {
                    i.remove();
                }
            }
        }
    }

    private final void debug(String str) {
        System.out.println(this + ": " + str);
    }

    private final void debug(String str, Throwable e) {
        System.out.println(this + ": " + str);
        e.printStackTrace();
    }

}
