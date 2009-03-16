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
package org.apache.activemq.broker;

import java.beans.ExceptionListener;
import java.io.IOException;

import org.apache.activemq.Connection;
import org.apache.activemq.Service;
import org.apache.activemq.broker.openwire.OpenwireProtocolHandler;
import org.apache.activemq.broker.stomp.StompProtocolHandler;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.stomp.StompWireFormat;
import org.apache.activemq.wireformat.WireFormat;

public class BrokerConnection extends Connection {
    
    protected Broker broker;
    private ProtocolHandler protocolHandler;

    public interface ProtocolHandler extends Service {
        public void setConnection(BrokerConnection connection);
        public void onCommand(Object command);
        public void onException(Exception error);
        public void setWireFormat(WireFormat wf);
    }

    
    public BrokerConnection() {
        setExceptionListener(new ExceptionListener(){
            public void exceptionThrown(Exception error) {
                error.printStackTrace();
                try {
                    stop();
                } catch (Exception ignore) {
                }
            }
        });
    }
    
    public Broker getBroker() {
        return broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }
    
    
    @Override
    public boolean isStopping() {
        return super.isStopping() || broker.isStopping();
    }
    
    public void onCommand(Object command) {
        if( protocolHandler!=null ) {
            protocolHandler.onCommand(command);
        } else {
            try {
                
                // TODO: need to make this more extensible and dynamic.  Perhaps 
                // we should lookup the ProtocolHandler via a FactoryFinder
                WireFormat wf = (WireFormat) command;
                if( wf.getClass() == OpenWireFormat.class ) {
                    protocolHandler = new OpenwireProtocolHandler();
                } else if( wf.getClass() == StompWireFormat.class ) {
                    protocolHandler = new StompProtocolHandler();
                } else {
                    throw new IOException("No protocol handler available for: "+wf.getClass());
                }
                
                protocolHandler.setConnection(this);
                protocolHandler.setWireFormat(wf);
                protocolHandler.start();
                
                setExceptionListener(new ExceptionListener(){
                    public void exceptionThrown(Exception error) {
                        protocolHandler.onException(error);
                    }
                });
            } catch (Exception e) {
                onException(e);
            }
        }
    }
    
    @Override
    public void stop() throws Exception {
        super.stop();
        if( protocolHandler!=null ) {
            try {
                protocolHandler.stop();
            } catch (Exception ignore) {
            }
        }
    }
    
}
