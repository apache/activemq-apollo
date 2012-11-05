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
