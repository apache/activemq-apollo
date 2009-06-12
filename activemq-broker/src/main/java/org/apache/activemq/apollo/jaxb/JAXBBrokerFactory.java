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
import java.net.URL;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.activemq.apollo.broker.BrokerFactory;
import org.apache.activemq.apollo.broker.MessageBroker;
import org.apache.activemq.util.URISupport;

public class JAXBBrokerFactory implements BrokerFactory.Handler {

	public MessageBroker createBroker(URI brokerURI) throws Exception {
		JAXBContext context = JAXBContext.newInstance(Broker.class);
		Unmarshaller unmarshaller = context.createUnmarshaller();

		URL configURL;
		brokerURI = URISupport.stripScheme(brokerURI);
		String scheme = brokerURI.getScheme();
		if( scheme==null || "file".equals(scheme) ) {
			configURL = URISupport.changeScheme(brokerURI, "file").toURL();
		} else if( "classpath".equals(scheme) ) {
			configURL = Thread.currentThread().getContextClassLoader().getResource(brokerURI.getSchemeSpecificPart());
		} else {
			configURL = URISupport.changeScheme(brokerURI, scheme).toURL();
		}		
		
		Broker config = (Broker) unmarshaller.unmarshal(configURL);
		return config.createMessageBroker();
	}


}
