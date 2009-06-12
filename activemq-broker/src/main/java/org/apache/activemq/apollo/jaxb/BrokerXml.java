package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.apollo.broker.Broker;

@XmlRootElement(name="broker")
public class BrokerXml {

	public Broker createMessageBroker() {
		return null;
	}

}
