/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.activemq.apollo.jaxb

import java.io.IOException
import javax.xml.bind.JAXBContext
import javax.xml.stream.XMLInputFactory
import org.apache.activemq.util.URISupport
import java.net.{URL, URI}
import collection.JavaConversions._
import org.apache.activemq.apollo.broker._
import jaxb.PropertiesReader
import org.apache.activemq.apollo.dto._
import org.apache.activemq.transport.TransportFactory

class XmlBrokerFactory extends BrokerFactory.Handler {
  
  def createBroker(value: String): Broker = {
    try {
      var brokerURI = new URI(value)
      val context = JAXBContext.newInstance("org.apache.activemq.apollo.dto")
      val unmarshaller = context.createUnmarshaller()

      var configURL: URL = null
      brokerURI = URISupport.stripScheme(brokerURI)
      val scheme = brokerURI.getScheme()
      if (scheme == null || "file".equals(scheme)) {
        configURL = URISupport.changeScheme(URISupport.stripScheme(brokerURI), "file").toURL()
      } else
      if ("classpath".equals(scheme)) {
        configURL = Thread.currentThread().getContextClassLoader().getResource(brokerURI.getSchemeSpecificPart())
      } else {
        configURL = URISupport.changeScheme(brokerURI, scheme).toURL()
      }
      if (configURL == null) {
        throw new IOException("Cannot create broker from non-existent URI: " + brokerURI)
      }
      val factory = XMLInputFactory.newInstance()
      val reader = factory.createXMLStreamReader(configURL.openStream())
      val properties = new PropertiesReader(reader)
      val xml = unmarshaller.unmarshal(properties).asInstanceOf[BrokerDTO]
      return createMessageBroker(xml)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Cannot create broker from URI: " + value, e)
    }
  }

  def createMessageBroker(brokerModel: BrokerDTO): Broker = {
    val rc = new Broker()
    for (virtualHostModel <- brokerModel.virtualHosts) {
      rc.addVirtualHost(createVirtualHost(virtualHostModel))
    }
    for (connector <- brokerModel.connectors) {
      try {
        val server = TransportFactory.bind(connector.transport)
        rc.transportServers.add(server)
      } catch {
        case e:Exception=>
          throw new Exception("Unable to bind transport server '" + connector + " due to: " + e.getMessage(), e)
      }
    }
    for (connector <- brokerModel.connectors) {
      rc.connectUris.add(connector.advertise)
    }
    return rc
  }


  def createVirtualHost(virtualHostModel: VirtualHostDTO): VirtualHost = {
    val rc = new VirtualHost()
    rc.setNamesArray(virtualHostModel.hostNames)
    if (virtualHostModel.store != null) {
      val database = new BrokerDatabase()
      database.setVirtualHost(rc)
//      TODO:
//      database.setStore( )
      rc.setDatabase(database)
    }
    return rc
  }
}
