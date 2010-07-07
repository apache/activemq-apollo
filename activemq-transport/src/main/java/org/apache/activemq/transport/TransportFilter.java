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
package org.apache.activemq.transport;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtdispatch.DispatchQueue;

/**
 * @version $Revision: 1.5 $
 */
public class TransportFilter implements TransportListener, Transport {

    protected Transport next;
    protected TransportListener transportListener;

    public TransportFilter(Transport next) {
        this.next = next;
    }

    /**
     * @return Returns the next transport.
     */
    public Transport getNext() {
        return next;
    }

    public void setNext(Transport next) {
        this.next = next;
    }

    public TransportListener getTransportListener() {
        return transportListener;
    }

    public void setTransportListener(TransportListener listener) {
        this.transportListener = listener;
        if (listener == null) {
            next.setTransportListener(null);
        } else {
            next.setTransportListener(this);
        }
    }

    public DispatchQueue getDispatchQueue() {
        return next.getDispatchQueue();
    }

    public void setDispatchQueue(DispatchQueue queue) {
        next.setDispatchQueue(queue);
    }

    public void suspendRead() {
        next.suspendRead();
    }

    public void resumeRead() {
        next.resumeRead();
    }

    /**
     * @see org.apache.activemq.Service#start()
     * @throws IOException
     *             if the next channel has not been set.
     */
    public void start() throws Exception {
        if (next == null) {
            throw new IOException("The next transport has not been set.");
        }
        if (transportListener == null) {
            throw new IOException("The command listener has not been set.");
        }
        next.start();
    }

    /**
     * @see org.apache.activemq.Service#stop()
     */
    public void stop() throws Exception {
        next.stop();
    }

    public void onCommand(Object command) {
        transportListener.onCommand(command);
    }


    public String toString() {
        return next.toString();
    }

    @Deprecated
    public void oneway(Object command) {
        oneway(command, null);
    }

    public void oneway(Object command, CompletionCallback callback) {
        next.oneway(command, callback);
    }


    public void onException(IOException error) {
        transportListener.onException(error);
    }

    public void onDisconnected() {
        transportListener.onDisconnected();
    }

    public void onConnected() {
        transportListener.onConnected();
    }

    public <T> T narrow(Class<T> target) {
        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
        }
        return next.narrow(target);
    }

    public String getRemoteAddress() {
        return next.getRemoteAddress();
    }

    /**
     * @return
     * @see org.apache.activemq.transport.Transport#isFaultTolerant()
     */
    public boolean isFaultTolerant() {
        return next.isFaultTolerant();
    }

    public boolean isDisposed() {
        return next.isDisposed();
    }

    public boolean isConnected() {
        return next.isConnected();
    }

    public void reconnect(URI uri, CompletionCallback callback) {
        next.reconnect(uri, callback);
    }

    public WireFormat getWireformat() {
        return next.getWireformat();
    }
    public void setWireformat(WireFormat wireformat) {
        next.setWireformat(wireformat);
    }


}
