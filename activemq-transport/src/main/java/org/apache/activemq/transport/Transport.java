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

import org.apache.activemq.Service;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtdispatch.DispatchQueue;

/**
 * Represents an abstract connection.  It can be a client side or server side connection.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface Transport extends Service {

    @Deprecated
    void oneway(Object command);

    /**
     * A one way asynchronous send.  Once the command is transmitted the callback
     * is invoked.
     * 
     * @param command
     * @param callback
     * @throws IOException
     */
    void oneway(Object command, CompletionCallback callback);

    /**
     * Returns the current transport listener
     *
     * @return
     */
    TransportListener getTransportListener();

    /**
     * Registers an inbound command listener
     *
     * @param commandListener
     */
    void setTransportListener(TransportListener commandListener);

    /**
     * Returns the dispatch queue used by the transport
     *
     * @return
     */
    DispatchQueue getDispatchQueue();

    /**
     * Sets the dispatch queue used by the transport
     *
     * @param queue
     */
    void setDispatchQueue(DispatchQueue queue);

    /**
     * suspend delivery of commands.
     */
    void suspendRead();

    /**
     * resume delivery of commands.
     */
    void resumeRead();

    /**
     * @param target
     * @return the target
     */
    <T> T narrow(Class<T> target);

    /**
     * @return the remote address for this connection
     */
    String getRemoteAddress();

    /**
     * Indicates if the transport can handle faults
     * 
     * @return true if fault tolerant
     */
    boolean isFaultTolerant();

    /**
     * @return true if the transport is disposed
     */
    boolean isDisposed();
    
    /**
     * @return true if the transport is connected
     */
    boolean isConnected();
    
    /**
     * @return The wireformat for the connection.
     */
    WireFormat getWireformat();

    void setWireformat(WireFormat wireformat);

    /**
     * reconnect to another location
     * @param uri
     * @throws IOException on failure of if not supported
     */
    void reconnect(URI uri, CompletionCallback callback);

}
