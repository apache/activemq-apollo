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
package org.apache.activemq.apollo;

import java.beans.ExceptionListener;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.Service;
import org.apache.activemq.transport.CompletionCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;

abstract public class Connection implements TransportListener, Service {

    protected Transport transport;
    protected String name;

    protected DispatchQueue dispatchQueue = Dispatch.createQueue();
    protected boolean stopping;
    protected ExceptionListener exceptionListener;

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void start() throws Exception {
        transport.setTransportListener(this);
        transport.setDispatchQueue(dispatchQueue);
        transport.start();
    }

    public void stop() throws Exception {
        stopping = true;
        if (transport != null) {
            transport.stop();
        }
        dispatchQueue.release();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    protected void initialize() {
    }

    public final void write(final Object o) {
        write(o, null);
    }

    public final void write(final Object o, final CompletionCallback callback) {
        transport.oneway(o, callback);
    }

    final public void onException(IOException error) {
        if (!isStopping()) {
            onException((Exception) error);
        }
    }

    final public void onException(Exception error) {
        if (exceptionListener != null) {
            exceptionListener.exceptionThrown(error);
        }
    }

    public void setStopping() {
        stopping = true;
    }
    
    public boolean isStopping() {
        return stopping;
    }

    public void onDisconnected() {
    }

    public void onConnected() {
    }

    public DispatchQueue getDispatchQueue() {
        return dispatchQueue;
    }

    public Transport getTransport() {
        return transport;
    }

    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

}
