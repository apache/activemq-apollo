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
package org.apache.activemq.apollo.web

import com.google.inject.servlet.GuiceServletContextListener
import org.fusesource.scalate.guice.ScalateModule
import com.google.inject.Guice
import javax.servlet.ServletContextEvent
import org.apache.activemq.transport.TransportFactory
import org.apache.activemq.apollo.BrokerRegistry
import org.apache.activemq.apollo.broker.{CompletionTracker, Broker}

/**
 * A servlet context listener which registers
 * <a href="http://code.google.com/p/google-guice/wiki/Servlets">Guice Servlet</a>
 *
 * @version $Revision : 1.1 $
 */
class ServletContextListener extends GuiceServletContextListener {

  var broker:Broker = null

  override def contextInitialized(servletContextEvent: ServletContextEvent) = {

    // todo: replace this with info accessed from a configuration store.
    // register/start brokers we are managing.
    try {
      broker = createBroker();
      BrokerRegistry.add(broker);
      broker.start
    }
    catch {
      case e:Exception =>
        broker = null
        e.printStackTrace
    }

    super.contextInitialized(servletContextEvent);
  }

  def createBroker(): Broker = {
    val broker = new Broker()
    broker.name = "default"
    broker.transportServers.add(TransportFactory.bind("tcp://localhost:10000?wireFormat=multi"))
    broker.connectUris.add("tcp://localhost:10000")
    broker
  }

  override def contextDestroyed(servletContextEvent: ServletContextEvent) = {
    super.contextDestroyed(servletContextEvent);

    // un-register/stop brokers we are managing.
    if( broker!=null ) {
      val tracker = new CompletionTracker("broker shutdown")
      BrokerRegistry.remove(broker.name);
      broker.stop(tracker.task(broker.name))
      tracker.await()
    }
  }

  def getInjector = Guice.createInjector(new ScalateModule() {

    // TODO add some custom provider methods here
    // which can then be injected into resources or templates
    //
    // @Provides def createSomething = new MyThing()

    // lets add any package names which contain JAXRS resources
    // https://jersey.dev.java.net/issues/show_bug.cgi?id=485
    override def resourcePackageNames =
      "org.apache.activemq.apollo.web.resources" ::
      "org.codehaus.jackson.jaxrs" ::
      super.resourcePackageNames
  })


}