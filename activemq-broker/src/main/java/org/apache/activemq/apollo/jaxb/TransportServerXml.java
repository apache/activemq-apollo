package org.apache.activemq.apollo.jaxb;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;



@XmlRootElement(name="transportServer")
public class TransportServerXml {

	@XmlAttribute(name="uri")
	String uri;
	
	public TransportServer createTransportServer() throws IOException, URISyntaxException {
		return TransportFactory.bind(new URI(uri));
	}
	
}
