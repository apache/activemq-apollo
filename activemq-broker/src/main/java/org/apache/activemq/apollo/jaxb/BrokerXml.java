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
package org.apache.activemq.apollo.jaxb;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

@XmlRootElement(name="broker")
@XmlAccessorType(XmlAccessType.FIELD)
public class BrokerXml {
	
	@XmlAttribute(name="name")
	String name;
	
    @XmlElement(name="virtual-host")
    private List<VirtualHostXml> virtualHosts = new ArrayList<VirtualHostXml>();
    @XmlElement(name="transport-server")
    private List<String> transportServers = new ArrayList<String>();
    @XmlElement(name="connect-uri")
    private List<String> connectUris = new ArrayList<String>();
    @XmlElement(required = false)
    private DispatcherXml dispatcher;
	
	
	public Broker createMessageBroker() throws Exception {
		Broker rc = new Broker();
		if( dispatcher!=null ) {
			rc.setDispatcher(dispatcher.createDispatcher(this));
		}
		for (VirtualHostXml element : virtualHosts) {
			rc.addVirtualHost(element.createVirtualHost(this));
		}
		for (String element : transportServers) {
			TransportServer server;
			try {
				server = TransportFactory.bind(new URI(element));
			} catch (Exception e) {
				throw new Exception("Unable to bind transport server '"+element+" due to: "+e.getMessage(), e);
			}
			rc.addTransportServer(server);
		}
		for (String element : connectUris) {
			rc.addConnectUri(element);
		}
		
		return rc;
	}
	
	public VirtualHostXml getDefaultVirtualHost() {
		return null;
	}

	public List<VirtualHostXml> getVirtualHosts() {
		return virtualHosts;
	}
	public void setVirtualHosts(List<VirtualHostXml> virtualHosts) {
		this.virtualHosts = virtualHosts;
	}


	public List<String> getTransportServers() {
		return transportServers;
	}
	public void setTransportServers(List<String> transportServers) {
		this.transportServers = transportServers;
	}


	public List<String> getConnectUris() {
		return connectUris;
	}
	public void setConnectUris(List<String> connectUris) {
		this.connectUris = connectUris;
	}


	public DispatcherXml getDispatcher() {
		return dispatcher;
	}
	public void setDispatcher(DispatcherXml dispatcher) {
		this.dispatcher = dispatcher;
	}


}
