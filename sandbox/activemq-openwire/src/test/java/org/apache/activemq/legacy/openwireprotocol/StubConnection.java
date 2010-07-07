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
package org.apache.activemq.legacy.openwireprotocol;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.ShutdownInfo;

import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.apollo.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.ServiceSupport;

public class StubConnection implements Service {

    private final BlockingQueue<Object> dispatchQueue = new LinkedBlockingQueue<Object>();
    private ResponseCorrelator transport;
    private TransportListener listener;
    public AtomicReference<Throwable> error = new AtomicReference<Throwable>();

    public StubConnection(Transport transport) throws Exception {
        this.transport = new ResponseCorrelator(transport);
        transport.setTransportListener(new DefaultTransportListener() {
            public void onCommand(Object command) {
                try {
                    StubConnection.this.dispatch(command);
                } catch (Exception e) {
                    onException(new IOException("" + e));
                }
            }

            public void onException(IOException e) {
                if (listener != null) {
                    listener.onException(e);
                }
                error.set(e);
            }
        });
        transport.start();
    }

    protected void dispatch(Object command) throws InterruptedException, IOException {
        if (listener != null) {
            listener.onCommand(command);
        }
        dispatchQueue.put(command);
    }

    public BlockingQueue<Object> getDispatchQueue() {
        return dispatchQueue;
    }

    public void send(Command command) throws Exception {
        if (command instanceof Message) {
            Message message = (Message)command;
            message.setProducerId(message.getMessageId().getProducerId());
        }
        command.setResponseRequired(false);
        transport.oneway(command, null);
    }

    public Response request(Command command) throws Exception {
        if (command instanceof Message) {
            Message message = (Message)command;
            message.setProducerId(message.getMessageId().getProducerId());
        }
        command.setResponseRequired(true);
        Response response = (Response)transport.request(command);
        if (response != null && response.isException()) {
            ExceptionResponse er = (ExceptionResponse)response;
            throw JMSExceptionSupport.create(er.getException());
        }
        return response;
    }

    public Transport getTransport() {
        return transport;
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
        if (transport != null) {
            transport.oneway(new ShutdownInfo(), null);
            ServiceSupport.dispose(transport);
        }
    }

    public TransportListener getListener() {
        return listener;
    }

    public void setListener(TransportListener listener) {
        this.listener = listener;
    }
}
