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
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.type.messaging.Accepted;
import org.fusesource.hawtdispatch.Task;

import java.io.IOException;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public class AmqpListener {

    public Sasl processSaslConnect(TransportImpl protonTransport) {
        return null;
    }

    public Sasl processSaslEvent(Sasl sasl) {
        return sasl;
    }

    public void processConnectionOpen(Connection conn, Task onComplete) {
        conn.open();
        onComplete.run();
    }
    public void processConnectionClose(Connection conn, Task onComplete){
        conn.close();
        onComplete.run();
    }

    public void proccessSessionOpen(Session session, Task onComplete){
        session.open();
        onComplete.run();
    }
    public void processSessionClose(Session session, Task onComplete){
        session.close();
        onComplete.run();
    }

    public void processSenderOpen(Sender sender, Task onComplete) {
        sender.close();
        onComplete.run();
    }
    public void processSenderClose(Sender sender, Task onComplete){
        sender.close();
        onComplete.run();
    }

    public void processReceiverOpen(Receiver receiver, Task onComplete) {
        receiver.open();
        onComplete.run();
    }
    public void processReceiverClose(Receiver receiver, Task onComplete) {
        receiver.close();
        onComplete.run();
    }

    public void processDelivery(Receiver receiver, Delivery delivery){
    }

    public void processDelivery(Sender sender, Delivery delivery) {
    }

    public void processFailure(Throwable e) {
        e.printStackTrace();
    }

    public void processRefill() {
    }

    public void processTransportConnected() {
    }

    public void processTransportFailure(IOException e) {
        e.printStackTrace();
    }
}
