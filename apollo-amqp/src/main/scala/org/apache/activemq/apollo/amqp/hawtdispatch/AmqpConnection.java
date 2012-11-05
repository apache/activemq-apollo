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

package org.apache.activemq.apollo.amqp.hawtdispatch;

import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.engine.impl.ConnectionImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.transport.DefaultTransportListener;
import org.fusesource.hawtdispatch.transport.Transport;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integrates a proton transport/connection /w HawtDispatch transports.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class AmqpConnection {

    static final ExecutorService blockingExecutor = Executors.newCachedThreadPool();
    final ConnectionImpl protonConnection = new ConnectionImpl();

    Transport hawtdispatchTransport;
    TransportImpl protonTransport = new TransportImpl();
    HashSet<Object> endpointsBeingProcessed = new HashSet<Object>();

    public Sasl sasl;

    public void bind(Transport transport) throws Exception {
        this.protonTransport = new TransportImpl();
        this.protonTransport.setProtocolTracer(new ProtocolTracer() {
            public void receivedFrame(TransportFrame transportFrame) {
                System.out.println(String.format("RECV: %s:%05d | %s", hawtdispatchTransport.getRemoteAddress(), transportFrame.getChannel(), transportFrame.getBody()));
            }

            public void sentFrame(TransportFrame transportFrame) {
                System.out.println(String.format("SEND: %s:%05d | %s", hawtdispatchTransport.getRemoteAddress(), transportFrame.getChannel(), transportFrame.getBody()));
            }
        });
        this.protonTransport.bind(protonConnection);

        this.hawtdispatchTransport = transport;
        this.hawtdispatchTransport.setBlockingExecutor(blockingExecutor);
        if( this.hawtdispatchTransport.getProtocolCodec()==null ) {
            this.hawtdispatchTransport.setProtocolCodec(new AmqpProtocolCodec());
        }
        this.hawtdispatchTransport.setTransportListener(new DefaultTransportListener() {

            @Override
            public void onTransportConnected() {
                hawtdispatchTransport.resumeRead();
                listener.processTransportConnected();
            }

            @Override
            public void onTransportCommand(Object command) {
                try {
                    Buffer buffer;
                    if( command.getClass() == AmqpHeader.class ) {
                        AmqpHeader header = (AmqpHeader)command;
                        switch( header.getProtocolId() ) {
                            case 0:
                                break; // nothing to do..
                            case 3: // Client will be using SASL for auth..
                                sasl = listener.processSaslConnect(protonTransport);
                                break;
                            default:
                        }
                        buffer = header.getBuffer();
                    } else {
                        buffer = (Buffer) command;
                    }
                    protonTransport.input(buffer.data, buffer.offset, buffer.length);
                    fireListenerEvents();
                    pumpOut();
                } catch (Exception e) {
                    listener.processFailure(e);
                }
            }

            public void onRefill() {
                pumpOut();
            }

            @Override
            public void onTransportFailure(IOException error) {
                stop(Dispatch.NOOP);
                listener.processTransportFailure(error);
            }
        });
    }

    boolean hawtdispatchClosed = false;

    public static final EnumSet<EndpointState> UNINITIALIZED_SET = EnumSet.of(EndpointState.UNINITIALIZED);
    public static final EnumSet<EndpointState> INITIALIZED_SET = EnumSet.complementOf(UNINITIALIZED_SET);
    public static final EnumSet<EndpointState> ACTIVE_STATE = EnumSet.of(EndpointState.ACTIVE);
    public static final EnumSet<EndpointState> CLOSED_STATE = EnumSet.of(EndpointState.CLOSED);

    public void pumpOut() {
        if(hawtdispatchClosed) {
            return;
        }
        int size = hawtdispatchTransport.getProtocolCodec().getWriteBufferSize();
        byte data[] = new byte[size];
        boolean done = false;
        while( !done && !hawtdispatchTransport.full() ) {
            int count = protonTransport.output(data, 0, size);
            if( count > 0 ) {
                hawtdispatchTransport.offer(new Buffer(data, 0, count));
            } else {
                done = true;
            }
        }
        if( !hawtdispatchTransport.full() ) {
            listener.processRefill();
        }
    }

    AmqpListener listener = new AmqpListener();

    public DispatchQueue queue() {
        return getHawtdispatchTransport().getDispatchQueue();
    }

    class ProcessedTask extends Task {
        private final Object value;

        ProcessedTask(Object value) {
            this.value = value;
        }

        @Override
        public void run() {
            queue().assertExecuting();
            endpointsBeingProcessed.remove(value);
            pumpOut();
        }
    }


    public void fireListenerEvents() {

        if( sasl!=null ) {
            sasl = listener.processSaslEvent(sasl);
            if( sasl==null ) {
                // once sasl handshake is done.. we need to read the protocol header again.
                ((AmqpProtocolCodec)this.hawtdispatchTransport.getProtocolCodec()).readProtocolHeader();
            }
        }

        if(protonConnection.getLocalState() == EndpointState.UNINITIALIZED && protonConnection.getRemoteState() != EndpointState.UNINITIALIZED && !endpointsBeingProcessed.contains(protonConnection))
        {
            endpointsBeingProcessed.add(protonConnection);
            listener.processConnectionOpen(protonConnection, new ProcessedTask(protonConnection));
        }

        Session session = protonConnection.sessionHead(UNINITIALIZED_SET, INITIALIZED_SET);
        while(session != null)
        {
            if( !endpointsBeingProcessed.contains(session) ) {
                endpointsBeingProcessed.add(session);
                listener.proccessSessionOpen(session, new ProcessedTask(session));
            }
            session = session.next(UNINITIALIZED_SET, INITIALIZED_SET);
        }

        Link link = protonConnection.linkHead(UNINITIALIZED_SET, INITIALIZED_SET);
        while(link != null)
        {
            if( !endpointsBeingProcessed.contains(link) ) {
                endpointsBeingProcessed.add(link);
                link.setSource(link.getRemoteSource());
                link.setTarget(link.getRemoteTarget());
                ProcessedTask onComplete = new ProcessedTask(link);
                if( link instanceof Sender) {
                    listener.processSenderOpen((Sender) link, onComplete);
                } else {
                    listener.processReceiverOpen((Receiver) link, onComplete);
                }
            }
            link = link.next(UNINITIALIZED_SET, INITIALIZED_SET);
        }


        Delivery delivery = protonConnection.getWorkHead();
        while(delivery != null)
        {
            if(delivery.getLink() instanceof Receiver) {
                listener.processDelivery((Receiver) delivery.getLink(), delivery);
            } else {
                listener.processDelivery((Sender) delivery.getLink(), delivery);
            }
            delivery = delivery.getWorkNext();
        }

        link = protonConnection.linkHead(ACTIVE_STATE, CLOSED_STATE);
        while(link != null)
        {
            if( !endpointsBeingProcessed.contains(link) ) {
                endpointsBeingProcessed.add(link);
                ProcessedTask onComplete = new ProcessedTask(link);
                if( link instanceof Receiver) {
                    listener.processReceiverClose((Receiver) link, onComplete);
                } else {
                    listener.processSenderClose((Sender) link, onComplete);
                }
            }
            link = link.next(ACTIVE_STATE, CLOSED_STATE);
        }

        session = protonConnection.sessionHead(ACTIVE_STATE, CLOSED_STATE);
        while(session != null)
        {
            //TODO - close links?
            if( !endpointsBeingProcessed.contains(session) ) {
                endpointsBeingProcessed.add(session);
                listener.processSessionClose(session, new ProcessedTask(session));
            }
            session = session.next(ACTIVE_STATE, CLOSED_STATE);
        }
        if(protonConnection.getLocalState() == EndpointState.ACTIVE && protonConnection.getRemoteState() == EndpointState.CLOSED && !endpointsBeingProcessed.contains(protonConnection))
        {
            listener.processConnectionClose(protonConnection, new ProcessedTask(protonConnection));
            protonConnection.close();
        }

    }

    public AmqpListener getListener() {
        return listener;
    }

    public void setListener(AmqpListener listener) {
        this.listener = listener;
    }

    public void start(Task onComplete) {
        hawtdispatchTransport.start(onComplete);
    }

    public void stop(Task onComplete) {
        hawtdispatchClosed = true;
        hawtdispatchTransport.stop(onComplete);
    }

    public ConnectionImpl getProtonConnection() {
        return protonConnection;
    }

    public Transport getHawtdispatchTransport() {
        return hawtdispatchTransport;
    }

}
