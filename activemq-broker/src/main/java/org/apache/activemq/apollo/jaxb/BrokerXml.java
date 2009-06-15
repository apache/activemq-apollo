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

import java.util.concurrent.CopyOnWriteArrayList;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.apollo.broker.Broker;

@XmlRootElement(name="broker")
public class BrokerXml {
	
	@XmlAttribute(name="name")
	String name;
	
	@XmlElementWrapper(name="transportServers")
	@XmlElement(name="transportServer")
	CopyOnWriteArrayList<TransportServerXml> transportServers = new CopyOnWriteArrayList<TransportServerXml>();

	public Broker createMessageBroker() throws Exception {
		Broker broker = new Broker();
		broker.setName(name);
		for (TransportServerXml transportServer : transportServers) {
			broker.addTransportServer(transportServer.createTransportServer());
		}
		return broker;
	}

}
