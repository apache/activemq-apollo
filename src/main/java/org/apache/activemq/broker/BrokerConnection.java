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

import org.apache.activemq.Connection;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.broker.protocol.ProtocolHandlerFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.MultiWireFormatFactory.WireFormatConnected;

public class BrokerConnection extends Connection {
    
    protected Broker broker;
    private ProtocolHandler protocolHandler;

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

                WireFormatConnected wfconnected = (WireFormatConnected) command;
                String wfName = wfconnected.getWireFormatFactory().wireformatName();
                try {
                    protocolHandler = ProtocolHandlerFactory.createProtocolHandler(wfName);
                } catch(Exception e) {
                    throw IOExceptionSupport.create("No protocol handler available for: "+wfName, e);
                }
                
                protocolHandler.setConnection(this);
                protocolHandler.setWireFormat(wfconnected.getWireFormat());
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
