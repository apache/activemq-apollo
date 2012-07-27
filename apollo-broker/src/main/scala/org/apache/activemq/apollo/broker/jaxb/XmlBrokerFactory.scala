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

package org.apache.activemq.apollo.broker.jaxb

import java.io.IOException
import java.net.{URL, URI}
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.dto._
import java.lang.String
import XmlCodec._
import org.apache.activemq.apollo.util._
import java.util.Properties

class XmlBrokerFactory extends BrokerFactoryTrait {

  def createBroker(value:String, props:Properties): Broker = {
    try {
      var brokerURI = new URI(value)

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

      val broker = new Broker()
      broker.config = decode(classOf[BrokerDTO], configURL, props)
      return broker;

    } catch {
      case e: Exception =>
        throw new RuntimeException("Cannot create broker from URI: " + value, e)
    }
  }

}
