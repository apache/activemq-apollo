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
package org.apache.activemq.transport.nio;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import org.apache.activemq.transport.nio.SelectorManager.Listener;

/**
 * @author chirino
 */
public final class SelectorSelection {

    private final SelectorWorker worker;
    private final SelectionKey key;
    private final Listener listener;
    private boolean useChannelExecutor;
    private int interest;

    public SelectorSelection(SelectorWorker worker, SelectableChannel selectableChannel, Listener listener) throws ClosedChannelException {
        this.worker = worker;
        this.listener = listener;
        this.key = selectableChannel.register(worker.selector, 0, this);
        worker.incrementUseCounter();
    }

    public void setInterestOps(int ops) {
        interest = ops;
    }

    public void enable() {
        key.interestOps(interest);
        worker.selector.wakeup();
    }

    public void disable() {
        key.interestOps(0);
    }

    public void setUseChannelExecutor(boolean useChannelExecutor) {
        this.useChannelExecutor = useChannelExecutor;
    }

    public boolean getUseChannelExecutor() {
        return useChannelExecutor;
    }

    public void close() {
        worker.decrementUseCounter();
        key.cancel();
        worker.selector.wakeup();
    }

    public void onSelect() {
        listener.onSelect(this);
    }

    public void onError(Throwable e) {
        listener.onError(this, e);
    }

}
