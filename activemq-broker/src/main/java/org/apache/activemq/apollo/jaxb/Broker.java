package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.apollo.broker.MessageBroker;

@XmlRootElement(name="broker")
public class Broker {

	public MessageBroker createMessageBroker() {
		return null;
	}

}
