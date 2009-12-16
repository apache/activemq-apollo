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

import org.apache.activemq.dispatch.internal.advanced.DispatcherThread;

public class NIOSourceHandler {
    private final boolean DEBUG = false;

    private final Selector selector;
    private final DispatcherThread thread;

    public NIOSourceHandler(DispatcherThread thread) throws IOException {
        this.selector = Selector.open();
        this.thread = thread;
    }

    DispatcherThread getThread() {
        return thread;
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

    /**
     * Subclasses may override this to provide an alternative wakeup mechanism.
     */
    protected void wakeup() {
        selector.wakeup();
    }

    /**
     * Selects ready sources, potentially blocking. If wakeup is called during
     * select the method will return.
     * 
     * @param timeout
     *            A negative value cause the select to block until a source is
     *            ready, 0 will do a non blocking select. Otherwise the select
     *            will block up to timeout in milliseconds waiting for a source
     *            to become ready.
     * @throws IOException
     */
    public void doSelect(long timeout) throws IOException {

        try {
            if (timeout == -1) {
                selector.select();
            } else if (timeout > 0) {
                selector.select(timeout);
            } else {
                selector.selectNow();
            }
            processSelected();

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
                    NIODispatchSource source = (NIODispatchSource) key.attachment();

                    done = true;
                    try {
                        done = source.onSelect();
                    } catch (RuntimeException re) {
                        if (DEBUG)
                            debug("Exception in " + source + " canceling");
                        // If there is a Runtime error close the context:
                        // TODO better error handling here:
                        source.cancel();
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

    public void shutdown() throws IOException {
        for (SelectionKey key : selector.keys()) {
            NIODispatchSource source = (NIODispatchSource) key.attachment();
            source.cancel();
        }
        selector.close();
    }

    private final void debug(String str) {
        System.out.println(this + ": " + str);
    }

    private final void debug(String str, Throwable e) {
        System.out.println(this + ": " + str);
        e.printStackTrace();
    }

}
