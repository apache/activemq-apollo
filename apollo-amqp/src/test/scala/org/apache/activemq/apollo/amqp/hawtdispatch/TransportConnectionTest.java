/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.activemq.apollo.amqp.hawtdispatch;

import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.transport.*;
import org.junit.After;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.fusesource.hawtdispatch.Dispatch.NOOP;
import static org.fusesource.hawtdispatch.Dispatch.createQueue;
import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportConnectionTest {

    // Using a single dispatch queue for everything should simplify debugging issues.
    final DispatchQueue queue = createQueue();

    @Test
    public void testOpenSession() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);

        // Setup a little server...
        final TcpTransportServer server = startServer(new AmqpListener() {
            public void proccessSessionOpen(Session session, Task onComplete) {
                System.out.println("session opened..");
                session.open();
                done.countDown();
            }
        });
        final String address = server.getBoundAddress();

        // Start a client..
        queue.execute(new Task() {
            public void run() {
                try {
                    System.out.println("Creating a client connection.");
                    AmqpConnection c = startClient(address);
                    Session session = c.getProtonConnection().session();
                    session.open();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrue(done.await(3, TimeUnit.SECONDS));
    }

    final ArrayList<AmqpConnection> clients = new ArrayList<AmqpConnection>();
    synchronized private AmqpConnection startClient(String address) throws Exception {
        TcpTransport transport = new TcpTransport();
        transport.setDispatchQueue(queue);
        transport.connecting(new URI(address), null);
        final AmqpConnection clientConnection = new AmqpConnection();
        clientConnection.setListener(new AmqpListener(){
            @Override
            public void processTransportConnected() {
                clientConnection.pumpOut();
            }
        });
        clientConnection.bind(transport);
        clients.add(clientConnection);
        clientConnection.start(NOOP);
        clientConnection.getProtonConnection().open();
        return clientConnection;
    }

    @After
    synchronized public void stopClients() throws Exception {
        for (AmqpConnection client : clients) {
            stop(client);
        }
        clients.clear();
    }

    final ArrayList<TransportServer> servers = new ArrayList<TransportServer>();
    synchronized protected TcpTransportServer startServer(final AmqpListener serverHandler) throws Exception {
        final TcpTransportServer server = new TcpTransportServer(new URI("tcp://localhost:0"));
        server.setDispatchQueue(queue);
        server.setTransportServerListener(new TransportServerListener() {
            public void onAccept(Transport transport) throws Exception {
                System.out.println("Server accepted a client connection.");
                transport.setDispatchQueue(queue);
                AmqpConnection serverConnection = new AmqpConnection();
                serverConnection.bind(transport);
                serverConnection.setListener(serverHandler);
                serverConnection.start(NOOP);
            }

            public void onAcceptError(Exception error) {
                error.printStackTrace();
            }
        });
        start(server);
        servers.add(server);
        return server;
    }

    @After
    synchronized public void stopServers() throws Exception {
        for (TransportServer server : servers) {
            stop(server);
        }
        servers.clear();
    }

    private void start(TransportServer transport) throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        transport.start(new Task() {
            @Override
            public void run() {
                done.countDown();
            }
        });
        done.await();
    }
    private void stop(TransportServer transport) throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        transport.stop(new Task() {
            @Override
            public void run() {
                done.countDown();
            }
        });
        done.await();
    }

    private void start(AmqpConnection transport) throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        transport.start(new Task() {
            @Override
            public void run() {
                done.countDown();
            }
        });
        done.await();
    }
    private void stop(AmqpConnection transport) throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        transport.stop(new Task() {
            @Override
            public void run() {
                done.countDown();
            }
        });
        done.await();
    }
}
