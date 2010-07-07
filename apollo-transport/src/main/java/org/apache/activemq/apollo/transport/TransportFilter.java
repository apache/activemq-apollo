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
package org.apache.activemq.apollo.transport;

import org.fusesource.hawtdispatch.DispatchQueue;

import java.io.IOException;
import java.net.URI;

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
     * @see org.apache.activemq.apollo.util.Service#start()
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

    public void start(Runnable onComplete) throws Exception {
        next.start(onComplete);
    }

    /**
     * @see org.apache.activemq.apollo.util.Service#stop()
     */
    public void stop() throws Exception {
        next.stop();
    }

    public void stop(Runnable onComplete) throws Exception {
        next.stop(onComplete);
    }

    public void onTransportCommand(Object command) {
        transportListener.onTransportCommand(command);
    }

    public void onRefill() {
        transportListener.onRefill();
    }


    public String toString() {
        return next.toString();
    }

    public boolean  offer(Object command) {
        return next.offer(command);
    }

    public boolean full() {
        return next.full();
    }

    public void onTransportFailure(IOException error) {
        transportListener.onTransportFailure(error);
    }

    public void onTransportDisconnected() {
        transportListener.onTransportDisconnected();
    }

    public void onTransportConnected() {
        transportListener.onTransportConnected();
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
     * @see Transport#isFaultTolerant()
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

    public String getTypeId() {
        return next.getTypeId();
    }

    public void reconnect(URI uri) {
        next.reconnect(uri);
    }

    public ProtocolCodec getProtocolCodec() {
        return next.getProtocolCodec();
    }
    public void setProtocolCodec(ProtocolCodec protocolCodec) {
        next.setProtocolCodec(protocolCodec);
    }


}
