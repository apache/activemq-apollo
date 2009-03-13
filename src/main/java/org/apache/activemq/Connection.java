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
package org.apache.activemq;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

abstract public class Connection implements TransportListener {

    protected Transport transport;
    protected String name;

    private int priorityLevels;
    protected int outputWindowSize = 1000;
    protected int outputResumeThreshold = 900;
    protected int inputWindowSize = 1000;
    protected int inputResumeThreshold = 500;
    
    private IDispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private  ExecutorService blockingWriter;

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void start() throws Exception {
        transport.setTransportListener(this);
        if (transport instanceof DispatchableTransport) {
            DispatchableTransport dt = ((DispatchableTransport) transport);
            if (name != null) {
                dt.setName(name);
            }
            dt.setDispatcher(getDispatcher());
        } else {
            blockingWriter = Executors.newSingleThreadExecutor();
        }
        transport.start();
    }

    public void stop() throws Exception {
        stopping.set(true);
        if (transport != null) {
            transport.stop();
        }
        if (blockingWriter != null) {
            blockingWriter.shutdown();
        }
    }

    protected void initialize() {
    }
    
    public final void write(final Object o) {
        if (blockingWriter==null) {
            try {
                transport.oneway(o);
            } catch (IOException e) {
                onException(e);
            }
        } else {
            try {
                blockingWriter.execute(new Runnable() {
                    public void run() {
                        if (!stopping.get()) {
                            try {
                                transport.oneway(o);
                            } catch (IOException e) {
                                onException(e);
                            }
                        }
                    }
                });
            } catch (RejectedExecutionException re) {
                //Must be shutting down.
            }
        }
    }
    
    public void onException(IOException error) {
        if (!isStopping()) {
            onException((Exception) error);
        }
    }

    public void onException(Exception error) {
    }
    
    public boolean isStopping(){ 
        return stopping.get();
    }

    public void transportInterupted() {
    }

    public void transportResumed() {
    }

    public String getName() {
        return name;
    }

    public int getPriorityLevels() {
        return priorityLevels;
    }

    public void setPriorityLevels(int priorityLevels) {
        this.priorityLevels = priorityLevels;
    }

    public IDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(IDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public int getOutputWindowSize() {
        return outputWindowSize;
    }

    public int getOutputResumeThreshold() {
        return outputResumeThreshold;
    }

    public int getInputWindowSize() {
        return inputWindowSize;
    }

    public int getInputResumeThreshold() {
        return inputResumeThreshold;
    }

    public Transport getTransport() {
        return transport;
    }

}
